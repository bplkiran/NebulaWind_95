from datetime import datetime

from src.cleaning import clean_measurements


def test_invalid_and_missing_values_are_imputed(spark):
    rows = [
        (datetime(2022, 1, 1, 0), 1, 5.0, 90, 2.0, False),
        (datetime(2022, 1, 1, 1), 1, -1.0, 400, None, False),
    ]
    df = spark.createDataFrame(
        rows,
        ["timestamp","turbine_id","wind_speed","wind_direction","power_output","is_missing_record"]
    )
    out = clean_measurements(df).collect()
    assert all(r["power_output"] is not None for r in out)
    assert all(r["wind_speed"] is not None for r in out)
    assert all(r["wind_direction"] is not None for r in out)
    assert any(r["was_imputed"] for r in out)


def test_pi_based_conversion(spark):
    rows = [(datetime(2022,1,1), 1, 5.0, 180, 2.0, False)]
    df = spark.createDataFrame(
        rows,
        ["timestamp","turbine_id","wind_speed","wind_direction","power_output","is_missing_record"]
    )
    value = clean_measurements(df).first()["wind_direction_rad"]
    import math
    assert math.isclose(value, math.pi, rel_tol=1e-12)
