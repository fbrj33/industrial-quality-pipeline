# Industrial Quality Analytics Pipeline

An end-to-end data engineering project for monitoring semiconductor manufacturing quality using event streaming, distributed processing, statistical process control (SPC), and analytical data transformation.

The project uses the **SECOM semiconductor manufacturing dataset** to build a pipeline that ingests production measurements, processes them through Kafka and Spark Structured Streaming, stores them in PostgreSQL, and transforms them into analytics-ready datasets using dbt.

## Architecture

```mermaid
flowchart LR
    A["SECOM Dataset"] --> B["Python Producer"]
    B --> C["Apache Kafka"]
    C --> D["Spark Structured Streaming"]
    D --> E["PostgreSQL"]
    E --> F["dbt Staging"]
    F --> G["dbt Intermediate"]
    G --> H["dbt Analytical Marts"]
    H --> I["Power BI"]
```

The infrastructure is containerized locally using Docker Compose.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data preparation and event production |
| Pydantic | Event schema validation |
| Apache Kafka | Event streaming |
| Apache Spark Structured Streaming | Stream processing and SPC detection |
| PostgreSQL | Persistent storage |
| dbt Core | Data transformation and testing |
| Docker Compose | Local infrastructure |
| Power BI | Planned data visualization |

## Dataset

The project uses the SECOM dataset, containing semiconductor manufacturing measurements and corresponding quality labels.

- 1,567 production observations.
- 590 sensor measurement features.
- Quality labels:
  - `-1`: Pass
  - `1`: Fail

Eight sensors are selected for the initial pipeline:

- `sensor_060`
- `sensor_104`
- `sensor_130`
- `sensor_029`
- `sensor_511`
- `sensor_317`
- `sensor_125`
- `sensor_126`

## Pipeline Components

### 1. Data Ingestion

A Python producer reads the SECOM dataset, prepares production records, validates them using Pydantic, and publishes them to Kafka.

Each event contains:
- Event ID
- Event timestamp
- Production unit ID
- Eight selected sensor measurements
- Quality label

Kafka topic: `production-events`.

### 2. Event Streaming

Apache Kafka acts as the message broker between the producer and Spark processing application.

It decouples event production from event consumption and enables asynchronous processing.

### 3. Stream Processing and SPC

Spark Structured Streaming consumes production events from Kafka and processes them in micro-batches.

The application:
- Reads incoming production events.
- Processes the selected sensor measurements.
- Compares readings against historical sensor baselines.
- Detects readings outside three-sigma control limits.
- Persists production events and detected anomalies in PostgreSQL.
- Records batch processing metrics.

The three-sigma control limits are calculated using:

\[
UCL = \mu + 3\sigma
\]

\[
LCL = \mu - 3\sigma
\]

Where:
- \(\mu\) is the baseline sensor mean.
- \(\sigma\) is the baseline sensor standard deviation.

An SPC violation indicates a potentially unusual process measurement; it does not automatically mean that a production unit is defective.

### 4. PostgreSQL Storage

PostgreSQL stores production data, anomaly records, sensor baseline statistics, and pipeline processing metrics.

Main tables include:
- `production_events`
- `sensor_anomalies`
- `sensor_baseline_stats`
- `pipeline_batch_metrics`

### 5. dbt Transformations

dbt Core transforms the PostgreSQL data into reusable analytical models.

**Staging layer**
- `stg_production_events`: cleans and standardizes production event data.
- `stg_sensor_anomalies`: exposes Spark-generated anomaly records for analytics.

**Intermediate layer**
- `int_sensor_readings`: converts the eight sensor columns into individual sensor-reading records using a long-format structure.

**Analytical marts**
- `mart_daily_quality_metrics`: daily production volume, pass/fail counts, and quality rates.
- `mart_sensor_performance`: sensor statistics and comparisons between passed and failed production units.
- `mart_sensor_anomaly_summary`: anomaly counts grouped by date, sensor, anomaly type, and deviation type.

The project includes dbt data tests for uniqueness, missing values, accepted values, and relationships between production events and anomaly records.

## Benchmarking

The pipeline records batch processing metrics in PostgreSQL to support performance evaluation.

The benchmarking phase will evaluate:
- Processing duration.
- Event throughput.
- Batch processing performance.
- End-to-end processing latency.

Full-dataset benchmarking and final performance analysis are planned.

## Power BI Dashboard

The analytical marts are designed to support a Power BI dashboard containing:

- Production KPIs: total, passed, and failed units.
- Daily production quality trends.
- Failure and pass rates.
- Anomaly counts by sensor.
- Sensor performance comparisons.

Power BI dashboard implementation is planned.

## Running the Project

### Prerequisites

- Docker Desktop
- Python 3.11 or later
- Power BI Desktop (for visualization)
- Git

### 1. Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd industrial-quality-pipeline
```

### 2. Create and activate a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```powershell
pip install -r requirements.txt
```

### 4. Start infrastructure

```powershell
docker compose up -d
```

### 5. Calculate sensor baselines

```powershell
spark-submit src/streaming/calculate_sensor_baseline.py
```

Use the project's configured Spark environment and dependencies when running this script.

### 6. Start the Spark streaming consumer

Run the consumer using the configured Spark container:

```powershell
docker exec -it industrial-spark /opt/spark/bin/spark-submit `
  --conf spark.jars.ivy=/tmp/spark-ivy `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.5 `
  /opt/spark-app/src/streaming/spark_consumer.py
```

### 7. Publish production events

In another terminal:

```powershell
python scripts/produce_events.py --start 0 --limit 20 --interval 1
```

To publish all remaining observations:

```powershell
python scripts/produce_events.py --start 0 --limit 0 --interval 1
```

Run the producer with the Spark consumer active.

### 8. Run dbt transformations and tests

```powershell
dbt debug
dbt build
```

## Project Structure

```text
industrial-quality-pipeline/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── scripts/
│   └── produce_events.py
│
├── src/
│   ├── schemas/
│   │   └── production_event.py
│   └── streaming/
│       ├── calculate_sensor_baseline.py
│       ├── detect_spc_anomalies.py
│       └── spark_consumer.py
│
├── models/
│   ├── staging/
│   │   ├── stg_production_events.sql
│   │   └── stg_sensor_anomalies.sql
│   ├── intermediate/
│   │   └── int_sensor_readings.sql
│   ├── marts/
│   │   ├── mart_daily_quality_metrics.sql
│   │   ├── mart_sensor_performance.sql
│   │   └── mart_sensor_anomaly_summary.sql
│   └── schema.yml
│
├── docker-compose.yml
├── dbt_project.yml
├── requirements.txt
└── README.md
```

## Project Status

| Component | Status |
|---|---|
| SECOM data preparation | Implemented |
| Python event producer | Implemented |
| Kafka event streaming | Implemented |
| Spark Structured Streaming | Implemented |
| PostgreSQL persistence | Implemented |
| SPC three-sigma detection | Implemented |
| Batch processing metrics | Implemented |
| dbt staging models | Implemented |
| dbt intermediate models | Implemented |
| dbt analytical marts | Implemented |
| dbt data tests | Implemented |
| Power BI dashboard | Planned |
| Full-dataset processing and benchmarking | Pending |
| Final end-to-end validation | Pending |

## Future Improvements

- Complete the Power BI dashboard.
- Process the full SECOM dataset.
- Benchmark pipeline throughput and latency.
- Perform final end-to-end validation.
- Document experimental results and performance findings.

## Learning Objectives

This project explores practical data engineering concepts, including event-driven architectures, stream processing, distributed data transformation, relational data modeling, data quality testing, statistical process control, and pipeline performance evaluation.