# GridWatch

GridWatch is a data engineering project that collects and processes hourly U.S. electricity grid data from the Energy Information Administration (EIA).

The project uses Python, Apache Kafka, Apache Spark, PostgreSQL, and Docker to build a data pipeline for electricity demand, generation, interchange, and forecasting analytics.

## Architecture

EIA API → Python Ingestion → Apache Kafka → Apache Spark → PostgreSQL → Dashboard

## Tech Stack

- **Python:** API integration, pagination, and data validation
- **Apache Kafka:** Event streaming and message buffering
- **Apache Spark:** Stream processing (planned)
- **PostgreSQL:** Structured data storage
- **Docker Compose:** Local infrastructure orchestration


## Data Source

U.S. Energy Information Administration (EIA)

https://www.eia.gov/opendata/

## Local Setup

1. Install Python and Docker Desktop.
2. Install dependencies using `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and configure the required variables.
4. Start the services using `docker compose up -d`.
5. Create the `electricity.raw` Kafka topic with three partitions and replication factor 1.
6. Run `python ingestion/kafka_producer.py` to publish a test batch.

The Kafka producer currently processes a one-hour test window. Automated scheduling and persistent checkpoints will be implemented in a later stage.
