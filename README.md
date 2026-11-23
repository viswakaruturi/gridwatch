# GridWatch

**An end-to-end data engineering pipeline for U.S. electricity grid monitoring and analytics.**

GridWatch ingests hourly electricity data from the [U.S. Energy Information Administration (EIA)](https://www.eia.gov/opendata/), publishes records to Apache Kafka, processes them with Apache Spark Structured Streaming, and stores validated measurements in PostgreSQL. Apache Airflow coordinates incremental runs, recovery, and quality checks. A Streamlit dashboard and read-only FastAPI service make the processed data accessible for analysis.

> **Project status:** The local pipeline, Airflow orchestration, Streamlit dashboard, FastAPI service, and initial automated tests are implemented. Public cloud deployment is planned, not yet complete.

## Architecture

![GridWatch architecture diagram](docs/architecture.svg)

The core services run locally in Docker. Airflow starts the processing job directly using a compatible PySpark runtime and shares the persistent Spark checkpoint volume. The dashboard and API currently connect to local PostgreSQL; their cloud deployment is a separate planned phase.

## Tech stack

| Component | Technology | Role |
| --- | --- | --- |
| Data source | EIA Open Data API | Hourly U.S. grid measurements |
| Ingestion | Python, Requests | Pagination, validation, incremental backfill |
| Messaging | Apache Kafka, Confluent Python client | Buffered electricity events |
| Processing | Apache Spark 3.5.7 / PySpark Structured Streaming | Parse, validate, deduplicate, upsert |
| Storage | PostgreSQL 13 | Electricity records and analytical SQL views |
| Orchestration | Apache Airflow 3.1.8 | Scheduling, retries, dependencies, recovery |
| API | FastAPI | Read-only analytical endpoints |
| Visualization | Streamlit, pandas, Plotly | Interactive dashboard |
| Infrastructure | Docker Compose | Local services and persistent volumes |
| Testing | pytest, unittest, FastAPI TestClient | API and recovery tests |

## Electricity data

GridWatch ingests four EIA measurement types:

| Code | Measurement | Description |
| --- | --- | --- |
| `D` | Actual demand | Reported electricity demand |
| `DF` | Day-ahead demand forecast | Forecasted demand |
| `NG` | Net generation | Reported net electricity generation |
| `TI` | Total interchange | Positive net exports; negative net imports |

Energy readings are expressed in **MWh**. The grid-balance relationship used for quality analysis is `D = NG - TI`, where all values refer to the **same respondent and hour**.

**Interpretation cautions:** EIA balancing-authority records and regional aggregates can overlap; they must not all be summed as independent totals. Generation and interchange reporting can lag demand reporting. Missing or delayed measurements are not treated as zero.

## Pipeline and reliability

1. Airflow evaluates the processing window every 30 minutes using the most recent successful checkpoint.
2. If the latest due daily run is already complete, downstream processing is skipped.
3. Before ingestion, the readiness task checks Kafka and PostgreSQL connectivity.
4. Python retrieves completed UTC dates from EIA and publishes validated records to the Kafka topic `electricity.raw`.
5. Spark Structured Streaming uses `availableNow=True` and a persistent checkpoint to consume pending offsets, validate and deduplicate records, and upsert by `(period, respondent, type)`.
6. PostgreSQL quality validation checks for demand records on the target date, and logs per-type coverage.
7. The successful-run checkpoint advances only when the upstream tasks succeed.

Airflow's 30-minute schedule permits recovery after local downtime *once Docker and its services are running again*. It does **not** execute while the laptop is asleep or switched off. The local pipeline was observed recovering the missed November 20–22, 2026 dates and advancing its successful-run checkpoint to November 22. This is an observed local test, not a cloud uptime guarantee.

## SQL analytics

The `sql/views/` directory contains views covering:

- **Demand analytics:** Daily demand, hourly profiles, peak demand, rolling trends, weekday/weekend comparisons, and anomaly detection.
- **Forecasting:** Actual-versus-forecast errors, MAE/RMSE/MAPE summaries, and daily forecasting trends.
- **Grid operations:** Generation/demand balance, interchange patterns, and latest measurements.
- **Data quality:** Measurement coverage, internal missing-hour detection, and relative data freshness.

## Streamlit dashboard

The application has four pages:

1. **Grid Overview:** Latest demand, forecast, errors, trends, and freshness.
2. **Demand Analytics:** Daily and hourly patterns, peak events, weekday/weekend comparisons, and anomalies.
3. **Forecast Accuracy:** MAE, RMSE, MAPE, bias, and forecasting trends.
4. **Grid Operations:** Generation/interchange, grid balance, coverage, and data-quality checks.

Balancing-authority and date-range selections are shared across pages. Some plots intentionally show all available history, clearly identified in the interface.

Run locally from the repository root:

```powershell
python -m streamlit run dashboard/Grid_Overview.py
```

## FastAPI

Start the local API:

```powershell
python -m uvicorn api.main:app --reload
```

Interactive documentation: `http://127.0.0.1:8000/docs`

| Method | Endpoint | Description |
| --- | --- | --- |
| GET | `/health` | API and PostgreSQL health |
| GET | `/api/respondents` | Available respondents |
| GET | `/api/demand/{respondent}` | Hourly actual demand |
| GET | `/api/forecast-accuracy/{respondent}` | Aggregate forecast accuracy |
| GET | `/api/grid-operations/{respondent}` | Grid-balance measurements |
| GET | `/api/data-quality/{respondent}` | Freshness and daily coverage |

Example:

```text
GET /api/demand/TEPC?start_date=2026-11-20&end_date=2026-11-20&limit=24&offset=0
```

Demand and grid-operations endpoints accept optional `start_date`, `end_date`, `limit`, and `offset` parameters; the data-quality endpoint applies these to daily coverage. Forecast accuracy currently represents all available matched forecasts, not a user-selected date range. Unknown respondents and invalid parameters return appropriate HTTP errors.

## Local setup

### Prerequisites

- Docker Desktop with Docker Compose
- Python (the local API and tests have been exercised on Python 3.13)
- An [EIA API key](https://www.eia.gov/opendata/register.php)
- Sufficient RAM for Kafka, PostgreSQL, Spark, and Airflow

### 1. Configure the environment

```powershell
Copy-Item .env.example .env
python -m pip install -r requirements.txt
```

Set `EIA_API_KEY`, PostgreSQL credentials, and `KAFKA_CLUSTER_ID` in `.env`. The cluster ID must be a **valid Kafka KRaft cluster ID**, not the literal example placeholder. Never commit `.env`.

The sample environment uses `POSTGRES_HOST=localhost` for Python programs running on Windows. In Docker, Spark and Airflow override the host to `db` and use the Kafka internal endpoint `kafka:29092`.

### 2. Start core services

```powershell
docker compose up -d
```

Initialize the schema with `sql/schema.sql` and apply the project's SQL analytics/view scripts in dependency order to your PostgreSQL database. For example, when using the example credentials:

```powershell
Get-Content -Raw sql/schema.sql | docker compose exec -T db psql -v ON_ERROR_STOP=1 -U gridwatch_user -d gridwatch
```

Apply `sql/views/01_demand.sql` through `04_data_quality.sql` in numeric order. If you use different credentials, substitute your configured PostgreSQL username and database name. `sql/analytics.sql` contains additional analytical queries.

Create the Kafka topic if it does not already exist:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic electricity.raw --partitions 3 --replication-factor 1
```

### 3. Start Airflow

```powershell
docker compose -f airflow/compose.yaml up -d --build
```

Open `http://localhost:8080`, sign in using the credentials generated by the local Airflow standalone setup, and enable `gridwatch_pipeline`. The DAG checks every 30 minutes and runs ingestion only when processing is due.

**Security:** Airflow's `standalone` command and its embedded metadata database are intended here for local development, not an internet-exposed production deployment. Keep port 8080 bound to localhost.

### 4. Start the dashboard and API

Use separate terminals:

```powershell
python -m streamlit run dashboard/Grid_Overview.py
```

```powershell
python -m uvicorn api.main:app --reload
```

The dashboard and API need the local PostgreSQL container to be running.

## Tests

With PostgreSQL running and the `.env` values configured:

```powershell
python -m pytest tests/test_api.py -v
python -m unittest discover -s tests -p "test_recovery.py" -v
```

The API tests are **local database integration tests**, not isolated database-free unit tests. The recovery tests mock checkpoint reads and verify the date-range calculations. Additional Spark/error-injection tests can be added without altering the production pipeline.

## Repository layout

```text
gridwatch/
├── airflow/            # Docker image, Compose file, Airflow DAG
├── api/                # FastAPI service and database connection
├── dashboard/          # Four-page Streamlit app
├── ingestion/          # EIA collection, Kafka publication, backfill and recovery
├── spark/              # Structured Streaming processing and JDBC check
├── sql/                # Schema, analytics queries, analytical views
├── tests/              # API and recovery tests
├── compose.yaml        # Kafka, Spark and PostgreSQL
├── requirements.txt    # Local Python dependencies
└── .env.example        # Placeholder environment configuration
```

## Operational limitations and future deployment

- The compute pipeline runs locally and cannot execute while the laptop or Docker Desktop is offline; Airflow recovers missed dates when services are restored.
- A public Streamlit/FastAPI deployment and cloud-accessible PostgreSQL mirror remain **planned**; they are not included in the current local setup.
- A deployed cloud database would require a separate, reliable synchronization step from the local PostgreSQL instance.
- Streamlit and the API currently depend on local database credentials; do not publish those credentials or expose PostgreSQL directly to the public internet.
- EIA publication can be delayed, especially for generation/interchange measurements. Freshness is reported rather than silently filling missing values.

## License

No license has been selected yet. Add a license only after choosing the intended usage permissions.
