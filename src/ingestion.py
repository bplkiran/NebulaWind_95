import os
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType

from .config import GROUP_TO_TURBINES

SCHEMA = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("turbine_id", IntegerType(), False),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("power_output", DoubleType(), True),
])


def read_csvs(spark, input_dir):
    df = (
        spark.read.option("header", True).schema(SCHEMA)
        .csv(os.path.join(input_dir, "*.csv"))
        .withColumn("_source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
    )
    if df.limit(1).count() == 0:
        raise ValueError(f"No CSV records found in {input_dir}")
    return df


def validate_turbine_file_mapping(df):
    invalid = []
    for filename, turbines in GROUP_TO_TURBINES.items():
        # The production source has known group files. Test files may be named
        # differently, so only validate known source group files.
        rows = df.filter(F.col("_source_file") == filename)
        if rows.limit(1).count():
            bad = rows.filter(~F.col("turbine_id").isin(turbines))
            if bad.limit(1).count():
                invalid.append(filename)
    if invalid:
        raise ValueError(f"Invalid turbine/file mapping: {invalid}")
    return df


def deduplicate(df):
    return df.dropDuplicates(["timestamp", "turbine_id"])


def build_expected_hourly_grid(df):
    bounds = df.agg(
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).first()

    if not bounds or bounds["min_ts"] is None:
        raise ValueError("Cannot build grid from empty data.")

    turbines = df.select("turbine_id").distinct()

    grid = turbines.withColumn(
        "timestamp",
        F.explode(
            F.sequence(
                F.lit(bounds["min_ts"]),
                F.lit(bounds["max_ts"]),
                F.expr("INTERVAL 1 HOUR")
            )
        )
    )

    columns = [
        "timestamp",
        "turbine_id",
        "wind_speed",
        "wind_direction",
        "power_output",
    ]

    # _source_file exists for production CSV ingestion,
    # but not necessarily for unit-test DataFrames.
    if "_source_file" in df.columns:
        columns.append("_source_file")

    return (
        grid.join(
            df.select(*columns),
            ["timestamp", "turbine_id"],
            "left"
        )
        .withColumn(
            "is_missing_record",
            F.col("power_output").isNull()
        )
    )
