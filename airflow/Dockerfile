
FROM apache/airflow:3.1.8

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.1.8" \
    "confluent-kafka>=2,<3" \
    "pyspark==3.5.7"
