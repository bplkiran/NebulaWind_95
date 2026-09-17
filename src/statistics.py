from pyspark.sql import functions as F


def daily_summary(cleaned):
    return (
        cleaned.withColumn("event_date", F.to_date("timestamp"))
        .groupBy("event_date", "turbine_id")
        .agg(
            F.min("power_output").alias("min_power_mw"),
            F.max("power_output").alias("max_power_mw"),
            F.avg("power_output").alias("avg_power_mw"),
            F.stddev_pop("power_output").alias("std_power_mw"),
            F.count("*").alias("measurement_count"),
            F.sum(F.col("was_imputed").cast("int")).alias("imputed_count"),
        )
    )
