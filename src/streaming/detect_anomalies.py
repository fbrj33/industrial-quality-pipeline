
from pyspark.sql import SparkSession, functions as F
from functools import reduce

# PostgreSQL service hostname inside Docker Compose.
JDBC_URL = "jdbc:postgresql://postgres:5432/quality_db"

DB_PROPERTIES = {
    "user": "quality_user",
    "password": "local_dev_password",
    "driver": "org.postgresql.Driver",
}

SENSORS = [
    "sensor_060",
    "sensor_104",
    "sensor_130",
    "sensor_029",
    "sensor_511",
    "sensor_317",
    "sensor_125",
    "sensor_126",
]

STAGING_TABLE = "public.sensor_anomalies_staging"


def main():
    spark = (
        SparkSession.builder
        .appName("IndustrialQuality-SPC-AnomalyDetection")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    connection = None

    try:
        print("Reading production events...")

        events = spark.read.jdbc(
            url=JDBC_URL,
            table="public.production_events",
            properties=DB_PROPERTIES,
        )

        print("Reading sensor baselines...")

        baseline = spark.read.jdbc(
            url=JDBC_URL,
            table="public.sensor_baseline_stats",
            properties=DB_PROPERTIES,
        ).select(
            "sensor_name",
            "baseline_mean",
            "lower_control_limit",
            "upper_control_limit",
        )

        if baseline.isEmpty():
            raise RuntimeError(
                "No sensor baselines found. "
                "Run calculate_sensor_baseline.py first."
            )

        # Convert the wide production table into individual sensor readings.
        sensor_frames = [
            events.select(
                "event_id",
                "event_timestamp",
                F.col("unit_id").cast("string").alias("unit_id"),
                F.lit(sensor).alias("sensor_name"),
                F.col(sensor).cast("double").alias("sensor_value"),
            )
            for sensor in SENSORS
        ]

        readings = reduce(
            lambda left, right: left.unionByName(right),
            sensor_frames,
        )

        print("Comparing readings against SPC control limits...")

        anomalies = (
            readings
            .join(baseline, on="sensor_name", how="inner")
            .filter(
                F.col("sensor_value").isNotNull()
                & ~F.isnan("sensor_value")
                & (
                    (F.col("sensor_value") < F.col("lower_control_limit"))
                    | (F.col("sensor_value") > F.col("upper_control_limit"))
                )
            )
            .withColumn(
                "anomaly_type",
                F.lit("SPC_CONTROL_LIMIT"),
            )
            .withColumn(
                "deviation_type",
                F.when(
                    F.col("sensor_value") < F.col("lower_control_limit"),
                    F.lit("BELOW_LCL"),
                ).otherwise(F.lit("ABOVE_UCL")),
            )
            .select(
                "event_id",
                "event_timestamp",
                "unit_id",
                "sensor_name",
                "sensor_value",
                "baseline_mean",
                "lower_control_limit",
                "upper_control_limit",
                "anomaly_type",
                "deviation_type",
            )
            .dropDuplicates(["event_id", "sensor_name"])
        )

        detected_count = anomalies.count()

        print(f"Detected anomaly records: {detected_count}")
        anomalies.show(30, truncate=False)

        # Recreate staging table with the correct PostgreSQL schema.
        print("Preparing staging table...")

        connection = spark._jvm.java.sql.DriverManager.getConnection(
            JDBC_URL,
            DB_PROPERTIES["user"],
            DB_PROPERTIES["password"],
        )

        connection.setAutoCommit(False)

        try:
            statement = connection.createStatement()

            statement.executeUpdate("""
                DROP TABLE IF EXISTS public.sensor_anomalies_staging
            """)

            statement.executeUpdate("""
                CREATE TABLE public.sensor_anomalies_staging (
                    event_id VARCHAR(100) NOT NULL,
                    event_timestamp TIMESTAMPTZ,
                    unit_id VARCHAR,
                    sensor_name VARCHAR(50) NOT NULL,
                    sensor_value DOUBLE PRECISION NOT NULL,
                    baseline_mean DOUBLE PRECISION NOT NULL,
                    lower_control_limit DOUBLE PRECISION NOT NULL,
                    upper_control_limit DOUBLE PRECISION NOT NULL,
                    anomaly_type VARCHAR(50) NOT NULL,
                    deviation_type VARCHAR(20)
                )
            """)

            connection.commit()
            statement.close()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()
            connection = None

        # Write anomalies into staging.
        print("Writing anomalies to staging table...")

        anomalies.write.jdbc(
            url=JDBC_URL,
            table=STAGING_TABLE,
            mode="append",
            properties=DB_PROPERTIES,
        )

        # Insert new anomalies into the permanent table.
        print("Inserting anomalies into sensor_anomalies...")

        connection = spark._jvm.java.sql.DriverManager.getConnection(
            JDBC_URL,
            DB_PROPERTIES["user"],
            DB_PROPERTIES["password"],
        )

        connection.setAutoCommit(False)

        try:
            statement = connection.createStatement()

            statement.executeUpdate("""
                INSERT INTO public.sensor_anomalies (
                    event_id,
                    event_timestamp,
                    unit_id,
                    sensor_name,
                    sensor_value,
                    baseline_mean,
                    lower_control_limit,
                    upper_control_limit,
                    anomaly_type,
                    deviation_type
                )
                SELECT DISTINCT ON (event_id, sensor_name)
                    event_id,
                    event_timestamp,
                    unit_id,
                    sensor_name,
                    sensor_value,
                    baseline_mean,
                    lower_control_limit,
                    upper_control_limit,
                    anomaly_type,
                    deviation_type
                FROM public.sensor_anomalies_staging
                ORDER BY event_id, sensor_name
                ON CONFLICT (event_id, sensor_name) DO NOTHING
            """)

            connection.commit()

            statement.executeUpdate("""
                DROP TABLE IF EXISTS public.sensor_anomalies_staging
            """)

            connection.commit()
            statement.close()

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()
            connection = None

        print(
            "SPC anomaly detection completed successfully. "
            f"Processed {detected_count} anomaly records."
        )

    finally:
        if connection is not None:
            connection.close()

        spark.stop()


if __name__ == "__main__":
    main()
