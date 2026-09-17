# NebulaWind — Orion Wind Turbine Data Pipeline

## POC objective

NebulaWind is a Python + PySpark proof-of-concept for the supplied wind-turbine
telemetry. The **Orion** pipeline is deliberately small, testable and
production-oriented without adding unnecessary infrastructure.

### Requirements covered

- Python + PySpark
- Three CSV groups, each containing five fixed turbines
- Daily/24-hour processing
- Missing-record detection and median imputation
- Invalid-value and statistical-outlier handling
- Per-turbine daily min/max/average/stddev
- Fleet-based ±2 standard deviation anomaly detection
- Delta Lake storage
- Idempotent incremental writes using Delta `MERGE`
- Automated tests for ingestion, cleaning, statistics and anomalies
- `math.pi` used for all degree/radian calculations

## Source data

The supplied month contains:

- 3 CSV files
- 15 turbines
- March 1–31, 2022
- 744 hourly timestamps per turbine
- 11,160 measurement rows
- no nulls or duplicate `(timestamp, turbine_id)` keys

Because the supplied data is already clean, `data/test/data_group_test.csv`
contains intentionally corrupted records to exercise the cleaning logic.

## Architecture

```text
Raw CSVs
   |
   v
Ingestion + schema/file validation
   |
   v
Deduplicate measurement keys
   |
   v
Expected hourly grid
   |
   +--> missing-record detection
   |
   v
Cleaning
   +--> physical bounds
   +--> IQR outliers
   +--> turbine median imputation
   |
   v
Cleaned Delta table
   |
   +--> daily summary
   |
   +--> fleet statistics
            |
            +--> |z-score| > 2
                         |
                         v
                   anomalies Delta
```

## Project structure

```text
NebulaWind/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/
│   │   ├── data_group_1.csv
│   │   ├── data_group_2.csv
│   │   └── data_group_3.csv
│   └── test/
│       └── data_group_test.csv
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── ingestion.py
│   ├── cleaning.py
│   ├── statistics.py
│   ├── anomaly_detection.py
│   └── pipeline.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_ingestion.py
    ├── test_cleaning.py
    ├── test_statistics.py
    └── test_anomaly_detection.py
```

## Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the supplied month:

```bash
spark-submit --master local[*] src/pipeline.py \
  --input data/raw \
  --output output
```

Run the deliberately corrupted test dataset:

```bash
spark-submit --master local[*] src/pipeline.py \
  --input data/test \
  --output output_test
```

Run tests:

```bash
pytest -q
```

## Delta outputs

```text
output/delta/cleaned_measurements
output/delta/daily_summary
output/delta/anomalies
```

The cleaned table keeps audit columns including `is_missing_record`,
`was_imputed` and `cleaning_reason`.

## Key design decisions

### 1. Missing measurements

The source is hourly, so the pipeline creates an expected hourly timestamp
grid for every turbine between the batch's minimum and maximum timestamps.
Missing source rows become explicit rows with `is_missing_record = true`.

### 2. Imputation

Missing/invalid numeric values are replaced with the median for that turbine.
Median is chosen because it is less sensitive to extreme values than mean.

### 3. Outliers

For cleaning, an IQR rule is applied independently per turbine:

`lower = Q1 - 1.5 * IQR`
`upper = Q3 + 1.5 * IQR`

Outliers are set to null and then median-imputed. This is intentionally
separate from anomaly detection.

### 4. Anomalies

The assignment's ±2 standard deviation rule is applied to each turbine's
daily average power compared with the fleet's daily distribution:

`z = (turbine_daily_avg - fleet_daily_avg) / fleet_daily_std`

`abs(z) > 2` => anomaly.

This definition is a POC assumption; in production, expected output could be
modelled from wind speed, direction, turbine capacity, availability and
maintenance state.

### 5. Physical limits

The POC assumes:

- wind speed: 0–40 m/s
- direction: 0–359 degrees
- power: 0–5 MW

The 5 MW rating is a placeholder assumption and should come from turbine
metadata in production.

### 6. Pi

Direction conversion uses Python's `math.pi`:

`radians = degrees * math.pi / 180`

No hard-coded approximation such as 3.14 is used.

### 7. Incremental/idempotent processing

The pipeline writes measurement data using Delta `MERGE` on:

`timestamp + turbine_id`

If the same day's CSV is reprocessed, existing measurements are updated rather
than duplicated. Daily summaries and anomalies are also merged by their natural
keys.

## Productionising discussion

A production implementation could add:

- cloud object storage + Auto Loader/streaming ingestion
- orchestration with Databricks Workflows/Airflow
- turbine metadata and rated-capacity reference tables
- malformed-record quarantine
- schema evolution controls
- data-quality metrics and alerting
- partitioning/clustering strategy based on query patterns
- incremental watermarking
- data lineage and observability
- unit/integration tests in CI
- monitoring for late-arriving data and sensor downtime

The POC intentionally avoids ML because the requirement asks for a deterministic
data-engineering solution and minimal AI input.
