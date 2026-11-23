import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def get_engine():
    """Create a PostgreSQL connection engine."""

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="localhost",
        port=int(os.getenv("POSTGRES_HOST_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
    )

    return create_engine(url, pool_pre_ping=True)


def run_query(query, params=None):
    """Execute a SQL query and return a pandas DataFrame."""

    engine = get_engine()

    with engine.connect() as connection:
        return pd.read_sql_query(
            text(query),
            connection,
            params=params,
        )
