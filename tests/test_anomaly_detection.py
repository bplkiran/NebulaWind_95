from datetime import date

from src.anomaly_detection import flag_anomalies


def test_two_sigma_anomaly(spark):
    rows = [
        (date(2022,1,1),1,3.0,3.0,3.0,1.0,24,0),
        (date(2022,1,1),2,3.0,3.0,3.1,1.0,24,0),
        (date(2022,1,1),3,3.0,3.0,3.2,1.0,24,0),
        (date(2022,1,1),4,3.0,3.0,3.0,1.0,24,0),
        (date(2022,1,1),5,3.0,10.0,10.0,1.0,24,0),
    ]
    df=spark.createDataFrame(
        rows,
        ["event_date","turbine_id","min_power_mw","max_power_mw","avg_power_mw",
         "std_power_mw","measurement_count","imputed_count"]
    )
    out=flag_anomalies(df)
    assert out.filter("is_anomaly").select("turbine_id").first()["turbine_id"] == 5
