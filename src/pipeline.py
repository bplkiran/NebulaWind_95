import argparse
import os

from delta import DeltaTable
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .ingestion import read_csvs, validate_turbine_file_mapping, deduplicate, build_expected_hourly_grid
from .cleaning import clean_measurements
from .statistics import daily_summary
from .anomaly_detection import flag_anomalies


def build_spark():
    builder = (
        SparkSession.builder
        .appName("NebulaWind-Orion")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def merge_delta(df, path, keys):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not DeltaTable.isDeltaTable(df.sparkSession, path):
        df.write.format("delta").mode("overwrite").save(path)
        return

    target = DeltaTable.forPath(df.sparkSession, path)
    condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(input_dir, output_dir):
    spark = build_spark()
    try:
        raw = validate_turbine_file_mapping(
            deduplicate(read_csvs(spark, input_dir))
        )
        complete = build_expected_hourly_grid(raw)
        cleaned = clean_measurements(complete).cache()

        daily = daily_summary(cleaned).cache()
        scored = flag_anomalies(daily)
        anomalies = scored.filter("is_anomaly")

        delta_root = os.path.join(output_dir, "delta")
        merge_delta(cleaned, os.path.join(delta_root, "cleaned_measurements"),
                    ["timestamp", "turbine_id"])
        merge_delta(daily, os.path.join(delta_root, "daily_summary"),
                    ["event_date", "turbine_id"])
        merge_delta(anomalies, os.path.join(delta_root, "anomalies"),
                    ["event_date", "turbine_id"])

        print(f"Input rows after deduplication: {raw.count()}")
        print(f"Cleaned rows: {cleaned.count()}")
        print(f"Daily summary rows: {daily.count()}")
        print(f"Anomaly rows: {anomalies.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    run(args.input, args.output)
