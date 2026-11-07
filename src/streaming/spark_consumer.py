
import time

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

# PostgreSQL configuration
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

BATCH_METRICS_TABLE = "public.pipeline_batch_metrics"

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


def initialize_database(spark):
    """Create metrics/staging tables and align unit_id types."""

    connection = get_connection(spark)

    try:
        statement = connection.createStatement()

        statement.executeUpdate("""
            CREATE TABLE IF NOT EXISTS public.sensor_anomalies_staging (
                event_id VARCHAR(100) NOT NULL,
                event_timestamp TIMESTAMPTZ,
                unit_id INTEGER,
                sensor_name VARCHAR(50) NOT NULL,
                sensor_value DOUBLE PRECISION NOT NULL,
                baseline_mean DOUBLE PRECISION NOT NULL,
                lower_control_limit DOUBLE PRECISION NOT NULL,
                upper_control_limit DOUBLE PRECISION NOT NULL,
                anomaly_type VARCHAR(50) NOT NULL,
                deviation_type VARCHAR(20)
            )
        """)

        statement.executeUpdate("""
            ALTER TABLE public.sensor_anomalies_staging
            ALTER COLUMN unit_id TYPE INTEGER
            USING NULLIF(unit_id::TEXT, '')::INTEGER
        """)

        statement.executeUpdate("""
            ALTER TABLE public.sensor_anomalies
            ALTER COLUMN unit_id TYPE INTEGER
            USING NULLIF(unit_id::TEXT, '')::INTEGER
        """)

        statement.executeUpdate("""
            CREATE TABLE IF NOT EXISTS public.pipeline_batch_metrics (
                batch_id BIGINT PRIMARY KEY,
                events_received INTEGER NOT NULL,
                events_inserted INTEGER NOT NULL,
                duplicate_events_skipped INTEGER NOT NULL,
                anomalies_detected INTEGER NOT NULL,
                anomalies_inserted INTEGER NOT NULL,
                duplicate_anomalies_skipped INTEGER NOT NULL,
                processing_duration_ms BIGINT NOT NULL,
                throughput_events_per_second DOUBLE PRECISION NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) NOT NULL
            )
        """)

        statement.close()

    finally:
        connection.close()


def clear_staging_tables(spark):
    connection = get_connection(spark)

    try:
        statement = connection.createStatement()
        statement.executeUpdate(f"TRUNCATE TABLE {EVENTS_STAGING}")
        statement.executeUpdate(f"TRUNCATE TABLE {ANOMALIES_STAGING}")
        statement.close()
    finally:
        connection.close()


def persist_batch_metrics(
    statement,
    batch_id,
    event_count,
    events_inserted,
    anomaly_count,
    anomalies_inserted,
    duration_ms,
    throughput,
):
    """Upsert metrics so a retried Spark batch does not duplicate its metrics."""

    statement.executeUpdate(f"""
        INSERT INTO {BATCH_METRICS_TABLE} (
            batch_id,
            events_received,
            events_inserted,
            duplicate_events_skipped,
            anomalies_detected,
            anomalies_inserted,
            duplicate_anomalies_skipped,
            processing_duration_ms,
            throughput_events_per_second,
            processed_at,
            status
        )
        VALUES (
            {int(batch_id)},
            {int(event_count)},
            {int(events_inserted)},
            {int(event_count - events_inserted)},
            {int(anomaly_count)},
            {int(anomalies_inserted)},
            {int(anomaly_count - anomalies_inserted)},
            {int(duration_ms)},
            {float(throughput)},
            CURRENT_TIMESTAMP,
            'SUCCESS'
        )
        ON CONFLICT (batch_id) DO UPDATE SET
            events_received = EXCLUDED.events_received,
            events_inserted = EXCLUDED.events_inserted,
            duplicate_events_skipped = EXCLUDED.duplicate_events_skipped,
            anomalies_detected = EXCLUDED.anomalies_detected,
            anomalies_inserted = EXCLUDED.anomalies_inserted,
            duplicate_anomalies_skipped = EXCLUDED.duplicate_anomalies_skipped,
            processing_duration_ms = EXCLUDED.processing_duration_ms,
            throughput_events_per_second = EXCLUDED.throughput_events_per_second,
            processed_at = CURRENT_TIMESTAMP,
            status = 'SUCCESS'
    """)


def write_batch(batch_df, batch_id, spark, baseline):
    """Persist events/anomalies and record per-batch metrics."""

    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no events")
        return

    batch_started = time.perf_counter()
    batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        event_count = batch_df.count()

        print(
            f"\nProcessing batch {batch_id}: "
            f"{event_count} events received"
        )

        # 1. Clear leftover staging rows.
        clear_staging_tables(spark)

        # 2. Stage incoming production events.
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

        # 3. Convert sensor columns into individual readings.
        sensor_frames = [
            batch_df.select(
                "event_id",
                "event_timestamp",
                F.col("unit_id").cast("integer").alias("unit_id"),
                F.lit(sensor).alias("sensor_name"),
                F.col(sensor).cast("double").alias("sensor_value"),
            )
            for sensor in SENSORS
        ]

        readings = sensor_frames[0]

        for frame in sensor_frames[1:]:
            readings = readings.unionByName(frame)

        # 4. Detect readings outside SPC control limits.
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
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        try:
            anomaly_count = anomalies.count()

            print(
                f"Batch {batch_id}: detected "
                f"{anomaly_count} SPC anomaly candidates"
            )

            # 5. Stage anomalies.
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

            # 6. Insert permanent records and metrics in one transaction.
            connection = get_connection(spark)
            connection.setAutoCommit(False)

            try:
                statement = connection.createStatement()

                events_inserted = statement.executeUpdate(f"""
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

                anomalies_inserted = statement.executeUpdate(f"""
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

                duration_ms = int(
                    (time.perf_counter() - batch_started) * 1000
                )

                duration_seconds = max(duration_ms / 1000.0, 0.001)
                throughput = event_count / duration_seconds

                persist_batch_metrics(
                    statement=statement,
                    batch_id=batch_id,
                    event_count=event_count,
                    events_inserted=events_inserted,
                    anomaly_count=anomaly_count,
                    anomalies_inserted=anomalies_inserted,
                    duration_ms=duration_ms,
                    throughput=throughput,
                )

                # 7. Clear staging tables within the transaction.
                statement.executeUpdate(
                    f"TRUNCATE TABLE {EVENTS_STAGING}"
                )
                statement.executeUpdate(
                    f"TRUNCATE TABLE {ANOMALIES_STAGING}"
                )

                connection.commit()
                statement.close()

                duplicate_events = event_count - events_inserted
                duplicate_anomalies = anomaly_count - anomalies_inserted

                print(f"Batch {batch_id}: SUCCESS")
                print(
                    f"  Events: {events_inserted} inserted, "
                    f"{duplicate_events} duplicates skipped"
                )
                print(
                    f"  Anomalies: {anomalies_inserted} inserted, "
                    f"{duplicate_anomalies} duplicates skipped "
                    f"({anomaly_count} detected)"
                )
                print(
                    f"  Duration: {duration_ms} ms | "
                    f"Throughput: {throughput:.2f} events/sec"
                )

            except Exception:
                connection.rollback()
                raise

            finally:
                connection.close()

        finally:
            anomalies.unpersist()

    except Exception as exc:
        print(f"Batch {batch_id}: FAILED - {exc}")
        raise

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
        # IMPORTANT: Load baselines FIRST using Spark JDBC.
        print("Loading SPC sensor baselines...")

        baseline = (
            spark.read.jdbc(
                url=JDBC_URL,
                table="public.sensor_baseline_stats",
                properties=DB_PROPERTIES,
            )
            .select(
                "sensor_name",
                "baseline_mean",
                "lower_control_limit",
                "upper_control_limit",
            )
            .cache()
        )

        if baseline.isEmpty():
            raise RuntimeError(
                "No sensor baselines found. "
                "Run calculate_sensor_baseline.py first."
            )

        print(f"Loaded {baseline.count()} sensor baselines.")

        # Initialize tables AFTER the successful JDBC read.
        print("Initializing database tables...")
        initialize_database(spark)
        print("Database initialization completed.")

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
