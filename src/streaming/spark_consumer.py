
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "production-events"

JDBC_URL = "jdbc:postgresql://postgres:5432/quality_db"
DB_PROPERTIES = {
    "user": "quality_user",
    "password": "local_dev_password",
    "driver": "org.postgresql.Driver",
}

MAIN_TABLE = "production_events"
STAGING_TABLE = "production_events_staging"

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

spark = (
    SparkSession.builder
    .appName("IndustrialQualityPipeline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("Starting Industrial Quality Spark Streaming...")
print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"PostgreSQL destination: {MAIN_TABLE}")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed_stream = (
    kafka_stream
    .select(from_json(col("value").cast("string"), event_schema).alias("event"))
    .select("event.*")
    .filter(col("event_id").isNotNull())
)


def write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: no events")
        return

    # First, write the batch to PostgreSQL staging.
    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", STAGING_TABLE)
        .option("user", DB_PROPERTIES["user"])
        .option("password", DB_PROPERTIES["password"])
        .option("driver", DB_PROPERTIES["driver"])
        .mode("append")
        .save()
    )

    # Then move new events into the main table.
    # ON CONFLICT prevents duplicate event IDs.
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        DB_PROPERTIES["user"],
        DB_PROPERTIES["password"],
    )

    try:
        statement = connection.createStatement()
        statement.executeUpdate(f"""
            INSERT INTO {MAIN_TABLE} (
                event_id, event_timestamp, unit_id,
                sensor_060, sensor_104, sensor_130, sensor_029,
                sensor_511, sensor_317, sensor_125, sensor_126,
                quality_label
            )
            SELECT DISTINCT ON (event_id)
                event_id, event_timestamp, unit_id,
                sensor_060, sensor_104, sensor_130, sensor_029,
                sensor_511, sensor_317, sensor_125, sensor_126,
                quality_label
            FROM {STAGING_TABLE}
            ORDER BY event_id, event_timestamp DESC
            ON CONFLICT (event_id) DO NOTHING
        """)

        statement.executeUpdate(f"TRUNCATE TABLE {STAGING_TABLE}")
        statement.close()
        print(f"Batch {batch_id}: persisted events to PostgreSQL")

    finally:
        connection.close()


query = (
    parsed_stream.writeStream
    .foreachBatch(write_batch)
    .option(
        "checkpointLocation",
        "/tmp/spark-checkpoints/production-events-postgres",
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
