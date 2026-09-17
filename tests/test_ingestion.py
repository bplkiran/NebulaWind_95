from datetime import datetime

from src.ingestion import deduplicate, build_expected_hourly_grid


def test_deduplicate(spark):
    rows = [
        (datetime(2022, 1, 1, 0), 1, 5.0, 90, 2.0),
        (datetime(2022, 1, 1, 0), 1, 5.0, 90, 2.0),
    ]
    df = spark.createDataFrame(rows, ["timestamp","turbine_id","wind_speed","wind_direction","power_output"])
    assert deduplicate(df).count() == 1


def test_missing_hour_is_created(spark):
    rows = [
        (datetime(2022, 1, 1, 0), 1, 5.0, 90, 2.0),
        (datetime(2022, 1, 1, 2), 1, 5.0, 90, 2.0),
    ]
    df = spark.createDataFrame(rows, ["timestamp","turbine_id","wind_speed","wind_direction","power_output"])
    out = build_expected_hourly_grid(df)
    missing = out.filter("timestamp = '2022-01-01 01:00:00'").first()
    assert missing["is_missing_record"] is True
