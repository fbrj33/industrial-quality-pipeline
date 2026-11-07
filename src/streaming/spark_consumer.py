
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType,
)
from pyspark.storagelevel import StorageLevel

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "production-events"

# PostgreSQL configuration (Docker internal hostname and port)
JDBC_URL = "jdbc:postgresql://postgres:5432/quality_db"

DB_PROPERTIES = {
    "user": "quality_user",
    "password": "local_dev_password",
    "driver": "org.postgresql.Driver",
}

EVENTS_TABLE = "public.production_events"
EVENTS_STAGING = "public.production_events_staging"

ANOMALIES_TABLE = "public.sensor_anomalies"
ANOMALIES_STAGING = "public.sensor_anomalies_staging"

CHECKPOINT_LOCATION = (
    "/tmp/spark-checkpoints/production-events-postgres"
)

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

# Incoming Kafka event schema
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), True),
    StructField("unit_id", IntegerType(), True),
    StructField("sensor_060", DoubleType(), True),
    StructField("sensor_104", DoubleType(), True),
    StructField("sensor_130", DoubleType(), True),
    StructField("sensor_029", DoubleType(), True),
    StructField("sensor_511", DoubleType(), True),
    StructField("sensor_317", DoubleType(), True),
    StructField("sensor_125", DoubleType(), True),
    StructField("sensor_126", DoubleType(), True),
    StructField("quality_label", IntegerType(), True),
])


def get_connection(spark):
    return spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        DB_PROPERTIES["user"],
        DB_PROPERTIES["password"],
    )


def initialize_anomaly_staging(spark):
    """Create the anomaly staging table if it does not exist."""

    connection = get_connection(spark)

    try:
        statement = connection.createStatement()

        statement.executeUpdate("""
            CREATE TABLE IF NOT EXISTS public.sensor_anomalies_staging (
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

        statement.close()

    finally:
        connection.close()


def clear_staging_tables(spark):
    """Remove leftover records before processing a new micro-batch."""

    connection = get_connection(spark)

    try:
        statement = connection.createStatement()

        statement.executeUpdate(
            f"TRUNCATE TABLE {EVENTS_STAGING}"
        )

        statement.executeUpdate(
            f"TRUNCATE TABLE {ANOMALIES_STAGING}"
        )

        statement.close()

    finally:
        connection.close()


def write_batch(batch_df, batch_id, spark, baseline):
    """Persist production events and their SPC anomalies."""

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no events")
        return

    batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        event_count = batch_df.count()

        print(
            f"\nProcessing batch {batch_id}: "
            f"{event_count} production events"
        )

        # Step 1: Clear staging tables.
        clear_staging_tables(spark)

        # Step 2: Write production events to staging.
        (
            batch_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", EVENTS_STAGING)
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .mode("append")
            .save()
        )

        # Step 3: Convert sensor columns into individual readings.
        sensor_frames = [
            batch_df.select(
                "event_id",
                "event_timestamp",
                F.col("unit_id").cast("string").alias("unit_id"),
                F.lit(sensor).alias("sensor_name"),
                F.col(sensor).cast("double").alias("sensor_value"),
            )
            for sensor in SENSORS
        ]

        readings = sensor_frames[0]

        for frame in sensor_frames[1:]:
            readings = readings.unionByName(frame)

        # Step 4: Compare readings against SPC baselines.
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

        anomaly_count = anomalies.count()

        print(
            f"Batch {batch_id}: detected "
            f"{anomaly_count} SPC anomalies"
        )

        # Step 5: Write detected anomalies to their staging table.
        if anomaly_count > 0:
            (
                anomalies.write
                .format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", ANOMALIES_STAGING)
                .option("user", DB_PROPERTIES["user"])
                .option("password", DB_PROPERTIES["password"])
                .option("driver", DB_PROPERTIES["driver"])
                .mode("append")
                .save()
            )

        # Step 6: Move both staging datasets into permanent tables.
        # Both inserts and staging cleanup are committed together.
        connection = get_connection(spark)
        connection.setAutoCommit(False)

        try:
            statement = connection.createStatement()

            statement.executeUpdate(f"""
                INSERT INTO {EVENTS_TABLE} (
                    event_id,
                    event_timestamp,
                    unit_id,
                    sensor_060,
                    sensor_104,
                    sensor_130,
                    sensor_029,
                    sensor_511,
                    sensor_317,
                    sensor_125,
                    sensor_126,
                    quality_label
                )
                SELECT DISTINCT ON (event_id)
                    event_id,
                    event_timestamp,
                    unit_id,
                    sensor_060,
                    sensor_104,
                    sensor_130,
                    sensor_029,
                    sensor_511,
                    sensor_317,
                    sensor_125,
                    sensor_126,
                    quality_label
                FROM {EVENTS_STAGING}
                ORDER BY event_id, event_timestamp DESC NULLS LAST
                ON CONFLICT (event_id) DO NOTHING
            """)

            statement.executeUpdate(f"""
                INSERT INTO {ANOMALIES_TABLE} (
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
                FROM {ANOMALIES_STAGING}
                ORDER BY event_id, sensor_name
                ON CONFLICT (event_id, sensor_name) DO NOTHING
            """)

            statement.executeUpdate(
                f"TRUNCATE TABLE {EVENTS_STAGING}"
            )

            statement.executeUpdate(
                f"TRUNCATE TABLE {ANOMALIES_STAGING}"
            )

            connection.commit()
            statement.close()

            print(
                f"Batch {batch_id}: successfully persisted "
                f"{event_count} events and {anomaly_count} anomalies."
            )

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("IndustrialQualityPipeline")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("Starting Industrial Quality Spark Streaming...")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"PostgreSQL destination: {EVENTS_TABLE}")

    try:
        # Load the SPC baselines once when the application starts.
        print("Loading SPC sensor baselines...")

        baseline = spark.read.jdbc(
            url=JDBC_URL,
            table="public.sensor_baseline_stats",
            properties=DB_PROPERTIES,
        ).select(
            "sensor_name",
            "baseline_mean",
            "lower_control_limit",
            "upper_control_limit",
        ).cache()

        if baseline.isEmpty():
            raise RuntimeError(
                "No sensor baselines found. "
                "Run calculate_sensor_baseline.py first."
            )

        print(f"Loaded {baseline.count()} sensor baselines.")

        initialize_anomaly_staging(spark)

        # Read Kafka stream.
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS,
            )
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        # Parse JSON events.
        parsed_stream = (
            kafka_stream
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    event_schema,
                ).alias("event")
            )
            .select("event.*")
            .filter(F.col("event_id").isNotNull())
        )

        # Process every micro-batch.
        query = (
            parsed_stream.writeStream
            .foreachBatch(
                lambda batch_df, batch_id: write_batch(
                    batch_df,
                    batch_id,
                    spark,
                    baseline,
                )
            )
            .option(
                "checkpointLocation",
                CHECKPOINT_LOCATION,
            )
            .trigger(processingTime="5 seconds")
            .start()
        )

        print("Streaming query started successfully.")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
