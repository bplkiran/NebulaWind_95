from pyspark.sql import functions as F
from .config import ANOMALY_Z_THRESHOLD


def flag_anomalies(daily):
    # Calculate fleet statistics excluding the turbine being scored.
    stats = daily.groupBy("event_date").agg(
        F.sum("avg_power_mw").alias("sum_power_mw"),
        F.sum(F.col("avg_power_mw") ** 2).alias("sum_power_sq"),
        F.count("avg_power_mw").alias("fleet_count"),
    )

    scored = daily.join(stats, "event_date")

    # Leave-one-out mean.
    scored = scored.withColumn(
        "other_count",
        F.col("fleet_count") - 1
    ).withColumn(
        "other_sum",
        F.col("sum_power_mw") - F.col("avg_power_mw")
    ).withColumn(
        "fleet_avg_power_mw",
        F.when(
            F.col("other_count") > 0,
            F.col("other_sum") / F.col("other_count")
        )
    )

    # Leave-one-out population variance:
    #
    # variance = E[x²] - E[x]²
    scored = scored.withColumn(
        "other_sum_sq",
        F.col("sum_power_sq") - F.col("avg_power_mw") ** 2
    ).withColumn(
        "fleet_variance",
        F.when(
            F.col("other_count") > 0,
            (
                F.col("other_sum_sq") / F.col("other_count")
                - F.col("fleet_avg_power_mw") ** 2
            )
        )
    ).withColumn(
        "fleet_variance",
        F.greatest(F.col("fleet_variance"), F.lit(0.0))
    ).withColumn(
        "fleet_std_power_mw",
        F.sqrt(F.col("fleet_variance"))
    )

    scored = scored.withColumn(
        "z_score",
        F.when(
            F.col("fleet_std_power_mw") > 0,
            (
                F.col("avg_power_mw")
                - F.col("fleet_avg_power_mw")
            ) / F.col("fleet_std_power_mw")
        )
    ).withColumn(
        "is_anomaly",
        F.coalesce(
            F.abs(F.col("z_score")) >= ANOMALY_Z_THRESHOLD,
            F.lit(False)
        )
    )

    return scored.drop(
        "sum_power_mw",
        "sum_power_sq",
        "fleet_count",
        "other_count",
        "other_sum",
        "other_sum_sq",
        "fleet_variance",
    )