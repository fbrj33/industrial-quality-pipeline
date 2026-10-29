# Industrial Quality Analytics Pipeline

An end-to-end data engineering project for processing semiconductor manufacturing data and monitoring production quality using streaming technologies and statistical process control (SPC).

The project uses the **SECOM semiconductor manufacturing dataset** to build a data pipeline that ingests production measurements, processes events in real time, stores them in PostgreSQL, and transforms them into analytics-ready datasets.

## Project Overview

Manufacturing processes generate large volumes of sensor measurements. Detecting quality issues early requires a reliable pipeline capable of ingesting, processing, storing, and analyzing production data.

This project explores how modern data engineering technologies can support industrial quality monitoring through a combination of batch data preparation, event streaming, stream processing, and analytical data modeling.

### Objectives

- Build an end-to-end data pipeline using modern data engineering technologies.
- Ingest production measurements and publish them as Kafka events.
- Process incoming events using Spark Structured Streaming.
- Persist processed production data in PostgreSQL.
- Transform raw production data into analytical models using dbt.
- Prepare quality metrics for visualization and industrial monitoring.
- Explore Statistical Process Control (SPC) to identify potential production anomalies.

## Architecture

```mermaid
flowchart LR
    A["SECOM Dataset"] --> B["Python Producer"]
    B --> C["Apache Kafka"]
    C --> D["Spark Structured Streaming"]
    D --> E["PostgreSQL"]
    E --> F["dbt"]
    F --> G["Analytics-ready Models"]
    G --> H["Power BI"]
```

The pipeline is containerized locally using Docker Compose.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data preparation and event production |
| Pydantic | Event schema validation |
| Apache Kafka | Event streaming and message ingestion |
| Apache Spark | Distributed stream processing |
| PostgreSQL | Persistent storage |
| dbt Core | Data transformation and testing |
| Docker Compose | Local infrastructure and service orchestration |
| Power BI | Planned data visualization |

## Dataset

The project uses the **SECOM dataset**, which contains semiconductor manufacturing measurements and corresponding pass/fail labels.

Dataset characteristics:
- 1,567 production records.
- 590 sensor measurement features.
- Binary quality labels:
  - `-1`: Pass
  - `1`: Fail

To keep the initial pipeline manageable, eight sensors were selected for processing:

` sensor_060`, `sensor_104`, `sensor_130`, `sensor_029`, `sensor_511`, `sensor_317`, `sensor_125`, `sensor_126`

The selection considers sensor variability, missing values, and their relationship with production quality.

## Pipeline Workflow

### 1. Data Ingestion and Event Production

A Python producer reads the SECOM dataset, prepares production records, validates them against a Pydantic event schema, and publishes them to Kafka.

Each event contains:
- Event ID
- Event timestamp
- Production unit ID
- Selected sensor measurements
- Quality label

Kafka topic: `production-events`

### 2. Event Streaming with Apache Kafka

Apache Kafka acts as the event streaming layer between the Python producer and the processing engine.

It decouples data production from data processing and allows production events to be consumed asynchronously.

### 3. Stream Processing with Apache Spark

Spark Structured Streaming consumes events from Kafka and processes the incoming production measurements.

The processed events are written to PostgreSQL for persistent storage and downstream analysis.

### 4. Data Storage with PostgreSQL

PostgreSQL stores the processed production events in a structured relational table.

The main `production_events` table contains the event metadata, selected sensor measurements, quality labels, and ingestion timestamps.

### 5. Data Transformation with dbt

dbt Core is used to transform stored production data into analytics-ready models.

Current models include:

- **`stg_production_events`**: Standardizes production event data and derives a readable quality status (`PASS` / `FAIL`).
- **`daily_quality_metrics`**: Aggregates production records by date and calculates total units, passed units, failed units, and quality rates.

These models provide a foundation for downstream quality analysis and reporting.

## Statistical Process Control

A key objective of the project is to introduce Statistical Process Control (SPC) techniques to monitor manufacturing quality.

The planned monitoring logic includes:

- **Three-sigma control limits:** Identify measurements outside the mean ± 3 standard deviations.
- **Eight consecutive points:** Detect potential process shifts when eight consecutive measurements fall on the same side of the mean.

These rules will help identify potential out-of-control production behavior.

## Project Structure

```text
industrial-quality-pipeline/
│
├── data/
│   └── raw/                    # SECOM dataset files
│
├── scripts/
│   └── produce_events.py       # Kafka event producer
│
├── src/
│   ├── schemas/                # Pydantic event schemas
│   └── streaming/              # Spark streaming application
│
├── models/
│   └── staging/
│       └── stg_production_events.sql
│
├── docker-compose.yml          # Local infrastructure
├── dbt_project.yml             # dbt project configuration
├── requirements.txt
└── README.md
```

## Running