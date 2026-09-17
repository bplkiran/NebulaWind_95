from pyspark.sql import functions as F
from .config import MAX_POWER_MW, MAX_WIND_SPEED_MS, IQR_MULTIPLIER, PI


def clean_measurements(df):
    # Physical validity first.
    df = (
        df.withColumn(
            "_valid_wind_speed",
            F.when((F.col("wind_speed") >= 0) &
                   (F.col("wind_speed") <= MAX_WIND_SPEED_MS),
                   F.col("wind_speed"))
        )
        .withColumn(
            "_valid_direction",
            F.when((F.col("wind_direction") >= 0) &
                   (F.col("wind_direction") < 360),
                   F.col("wind_direction"))
        )
        .withColumn(
            "_valid_power",
            F.when((F.col("power_output") >= 0) &
                   (F.col("power_output") <= MAX_POWER_MW),
                   F.col("power_output"))
        )
    )

    # Robust statistical bounds by turbine.
    bounds = df.groupBy("turbine_id").agg(
        F.expr("percentile_approx(_valid_power, 0.25)").alias("power_q1"),
        F.expr("percentile_approx(_valid_power, 0.75)").alias("power_q3"),
        F.expr("percentile_approx(_valid_wind_speed, 0.25)").alias("ws_q1"),
        F.expr("percentile_approx(_valid_wind_speed, 0.75)").alias("ws_q3"),
    ).withColumn("power_iqr", F.col("power_q3") - F.col("power_q1")) \
     .withColumn("ws_iqr", F.col("ws_q3") - F.col("ws_q1"))

    df = df.join(bounds, "turbine_id", "left")

    df = (
        df.withColumn(
            "_clean_power",
            F.when(
                (F.col("_valid_power") >= F.col("power_q1") - IQR_MULTIPLIER * F.col("power_iqr")) &
                (F.col("_valid_power") <= F.col("power_q3") + IQR_MULTIPLIER * F.col("power_iqr")),
                F.col("_valid_power")
            )
        )
        .withColumn(
            "_clean_wind_speed",
            F.when(
                (F.col("_valid_wind_speed") >= F.col("ws_q1") - IQR_MULTIPLIER * F.col("ws_iqr")) &
                (F.col("_valid_wind_speed") <= F.col("ws_q3") + IQR_MULTIPLIER * F.col("ws_iqr")),
                F.col("_valid_wind_speed")
            )
        )
    )

    medians = df.groupBy("turbine_id").agg(
        F.expr("percentile_approx(_clean_power, 0.5)").alias("power_median"),
        F.expr("percentile_approx(_clean_wind_speed, 0.5)").alias("wind_speed_median"),
        F.expr("percentile_approx(_valid_direction, 0.5)").alias("direction_median"),
    )

    df = df.join(medians, "turbine_id", "left")

    # Audit before replacement.
    reason = (
        F.when(F.col("is_missing_record"), F.lit("missing_record"))
         .when(F.col("_valid_power").isNull(), F.lit("invalid_power"))
         .when(F.col("_clean_power").isNull(), F.lit("power_outlier"))
         .when(F.col("_valid_wind_speed").isNull(), F.lit("invalid_wind_speed"))
         .when(F.col("_clean_wind_speed").isNull(), F.lit("wind_speed_outlier"))
         .when(F.col("_valid_direction").isNull(), F.lit("invalid_wind_direction"))
    )

    return (
        df.withColumn("cleaning_reason", reason)
          .withColumn("was_imputed", reason.isNotNull())
          .withColumn("power_output", F.coalesce(F.col("_clean_power"), F.col("power_median")))
          .withColumn("wind_speed", F.coalesce(F.col("_clean_wind_speed"), F.col("wind_speed_median")))
          .withColumn("wind_direction", F.coalesce(F.col("_valid_direction"), F.col("direction_median")))
          .withColumn(
              "wind_direction_rad",
              F.col("wind_direction") * F.lit(PI) / F.lit(180.0)
          )
          .drop(
              "_valid_wind_speed", "_valid_direction", "_valid_power",
              "power_q1", "power_q3", "ws_q1", "ws_q3",
              "power_iqr", "ws_iqr", "_clean_power", "_clean_wind_speed",
              "power_median", "wind_speed_median", "direction_median"
          )
    )
