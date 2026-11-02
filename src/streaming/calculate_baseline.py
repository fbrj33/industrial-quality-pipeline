
from pyspark.sql import SparkSession, functions as F

# Paths inside the Spark container
DATA_PATH = "/opt/spark-app/data/raw"

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

JDBC_URL = "jdbc:postgresql://industrial-postgres:5432/quality_db"

DB_PROPERTIES = {
    "user": "quality_user",
    "password": "local_dev_password",
    "driver": "org.postgresql.Driver",
}


def read_whitespace_file(spark, path):
    """Read a whitespace-separated file without using CSV delimiters."""
    return (
        spark.read.text(path)
        .select(
            F.split(F.trim(F.col("value")), r"\s+").alias("values")
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("SECOM-Sensor-Baseline")
        .getOrCreate()
    )

    try:
        # ---------------------------------------------------------
        # 1. Read historical sensor measurements
        # ---------------------------------------------------------
        measurements_raw = read_whitespace_file(
            spark, f"{DATA_PATH}/secom.data"
        )

        measurements = measurements_raw.select(
            *[
                F.col("values")[i].alias(f"sensor_{i + 1:03d}")
                for i in range(590)
            ]
        )

        if len(measurements.columns) != 590:
            raise ValueError(
                f"Expected 590 sensor columns, got {len(measurements.columns)}"
            )

        # ---------------------------------------------------------
        # 2. Read quality labels and timestamps
        # ---------------------------------------------------------
        labels_raw = read_whitespace_file(
            spark, f"{DATA_PATH}/secom_labels.data"
        )

        labels = labels_raw.select(
            F.regexp_extract(
                F.col("values")[0], r"-?\d+", 0
            ).cast("int").alias("quality_label"),
            F.col("values")[1].alias("event_timestamp"),
        )

        # ---------------------------------------------------------
        # 3. Align rows by their original file order
        # ---------------------------------------------------------
        # zipWithIndex assigns a stable index to each file's row
        # before joining measurements and labels.
        measurement_rdd = measurements.rdd.zipWithIndex().map(
            lambda item: (item[1], *item[0])
        )

        measurements_indexed = spark.createDataFrame(
            measurement_rdd,
            ["row_id"] + measurements.columns,
        )

        labels_rdd = labels.rdd.zipWithIndex().map(
            lambda item: (item[1], *item[0])
        )

        labels_indexed = spark.createDataFrame(
            labels_rdd,
            ["row_id", "quality_label", "event_timestamp"],
        )

        measurement_count = measurements_indexed.count()
        label_count = labels_indexed.count()

        if measurement_count != label_count:
            raise ValueError(
                f"Row count mismatch: {measurement_count} measurements, "
                f"{label_count} labels. Baseline calculation stopped."
            )

        df = measurements_indexed.join(
            labels_indexed,
            on="row_id",
            how="inner",
        )

        print(f"Historical measurement rows: {measurement_count}")
        print(f"Historical label rows: {label_count}")

        # ---------------------------------------------------------
        # 4. Convert selected sensor values to valid numeric values
        # ---------------------------------------------------------
        for sensor in SENSORS:
            value = F.col(sensor).cast("double")

            df = df.withColumn(
                sensor,
                F.when(
                    value.isNotNull() & ~F.isnan(value),
                    value,
                ).otherwise(F.lit(None).cast("double")),
            )

        # Use known-good units only for the reference baseline.
        good_units = df.filter(F.col("quality_label") == -1)

        good_count = good_units.count()
        print(f"Known-good historical units: {good_count}")

        # ---------------------------------------------------------
        # 5. Calculate statistics for each selected sensor
        # ---------------------------------------------------------
        rows = []

        for sensor in SENSORS:
            stats = good_units.agg(
                F.avg(F.col(sensor)).alias("mean"),
                F.stddev_samp(F.col(sensor)).alias("stddev"),
                F.count(F.col(sensor)).alias("sample_count"),
            ).first()

            mean = stats["mean"]
            stddev = stats["stddev"]
            sample_count = stats["sample_count"]

            if (
                mean is None
                or stddev is None
                or sample_count < 2
                or not float(mean) == float(mean)
                or not float(stddev) == float(stddev)
            ):
                print(
                    f"Skipping {sensor}: insufficient valid readings"
                )
                continue

            mean = float(mean)
            stddev = float(stddev)

            rows.append(
                (
                    sensor,
                    mean,
                    stddev,
                    mean - 3 * stddev,
                    mean + 3 * stddev,
                    int(sample_count),
                )
            )

        if not rows:
            raise RuntimeError(
                "No valid baseline statistics were calculated."
            )

        baseline = spark.createDataFrame(
            rows,
            [
                "sensor_name",
                "baseline_mean",
                "baseline_stddev",
                "lower_control_limit",
                "upper_control_limit",
                "baseline_sample_count",
            ],
        )

        print("Calculated sensor baseline:")
        baseline.orderBy("sensor_name").show(truncate=False)

        # ---------------------------------------------------------
        # 6. Save baseline statistics to PostgreSQL
        # ---------------------------------------------------------
        baseline.write.jdbc(
            url=JDBC_URL,
            table="sensor_baseline_stats",
            mode="overwrite",
            properties=DB_PROPERTIES,
        )

        print(
            f"Successfully stored baseline statistics for {len(rows)} sensors."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
