from datetime import datetime

from src.statistics import daily_summary


def test_daily_min_max_average(spark):
    rows = [
        (datetime(2022,1,1,0),1,1.0,90,1.0,False),
        (datetime(2022,1,1,1),1,2.0,90,2.0,False),
        (datetime(2022,1,1,2),1,3.0,90,4.0,False),
    ]
    df = spark.createDataFrame(
        rows,
        ["timestamp","turbine_id","wind_speed","wind_direction","power_output","was_imputed"]
    )
    r=daily_summary(df).first()
    assert r["min_power_mw"] == 1.0
    assert r["max_power_mw"] == 4.0
    assert r["avg_power_mw"] == 7/3
    assert r["measurement_count"] == 3
