from contextlib import closing
from datetime import date

from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor

from api.db import get_connection


app = FastAPI(
    title="GridWatch API",
    description="U.S. Electricity Grid Monitoring and Analytics",
    version="1.0.0",
)


def run_query(query, params=None):
    try:
        with closing(get_connection()) as connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

        return jsonable_encoder(rows)

    except OperationalError as exc:
        raise HTTPException(
            status_code=503,
            detail="Database unavailable",
        ) from exc


def validate_dates(start_date, end_date):
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="start_date cannot be after end_date",
        )


def validate_respondent(respondent):
    rows = run_query(
        """
        SELECT EXISTS (
            SELECT 1
            FROM electricity
            WHERE respondent = %s
        ) AS exists
        """,
        (respondent,),
    )

    if not rows[0]["exists"]:
        raise HTTPException(
            status_code=404,
            detail=f"Respondent '{respondent}' not found",
        )


@app.get("/health")
def health():
    rows = run_query("SELECT 1 AS database_check")

    if rows[0]["database_check"] != 1:
        raise HTTPException(
            status_code=503,
            detail="Database health check failed",
        )

    return {
        "status": "healthy",
        "service": "gridwatch-api",
        "database": "connected",
    }


@app.get("/api/respondents")
def get_respondents():
    rows = run_query(
        """
        SELECT
            respondent AS code,
            MAX(respondent_name) AS name
        FROM electricity
        GROUP BY respondent
        ORDER BY name
        """
    )

    return {
        "count": len(rows),
        "respondents": rows,
    }

@app.get("/")
def root():
    return {
        "project": "GridWatch",
        "description": "U.S. Electricity Grid Monitoring and Analytics",
        "status": "running",
        "documentation": "/docs",
        "health": "/health",
    }

@app.get("/api/demand/{respondent}")
def get_demand(
    respondent: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    validate_dates(start_date, end_date)
    validate_respondent(respondent)

    rows = run_query(
        """
        SELECT
            period,
            respondent,
            respondent_name,
            value AS demand_mwh,
            value_units
        FROM electricity
        WHERE respondent = %s
          AND type = 'D'
          AND (
              %s::date IS NULL
              OR period >= %s::date
          )
          AND (
              %s::date IS NULL
              OR period < (%s::date + INTERVAL '1 day')
          )
        ORDER BY period DESC
        LIMIT %s OFFSET %s
        """,
        (
            respondent,
            start_date,
            start_date,
            end_date,
            end_date,
            limit,
            offset,
        ),
    )

    return {
        "respondent": respondent,
        "start_date": start_date,
        "end_date": end_date,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "data": rows,
    }


@app.get("/api/forecast-accuracy/{respondent}")
def get_forecast_accuracy(respondent: str):
    validate_respondent(respondent)

    rows = run_query(
        """
        SELECT
            respondent,
            respondent_name,
            total_forecasts,
            mae_mwh,
            rmse_mwh,
            mape_percentage,
            mean_signed_error_mwh
        FROM vw_forecast_accuracy_summary
        WHERE respondent = %s
        """,
        (respondent,),
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No matched forecasts available for this respondent",
        )

    return rows[0]


@app.get("/api/grid-operations/{respondent}")
def get_grid_operations(
    respondent: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    validate_dates(start_date, end_date)
    validate_respondent(respondent)

    rows = run_query(
        """
        SELECT
            period,
            respondent,
            respondent_name,
            demand_mwh,
            generation_mwh,
            interchange_mwh,
            generation_minus_demand_mwh,
            balance_residual_mwh
        FROM vw_generation_demand_balance
        WHERE respondent = %s
          AND (
              %s::date IS NULL
              OR period >= %s::date
          )
          AND (
              %s::date IS NULL
              OR period < (%s::date + INTERVAL '1 day')
          )
        ORDER BY period DESC
        LIMIT %s OFFSET %s
        """,
        (
            respondent,
            start_date,
            start_date,
            end_date,
            end_date,
            limit,
            offset,
        ),
    )

    return {
        "respondent": respondent,
        "start_date": start_date,
        "end_date": end_date,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "data": rows,
    }


@app.get("/api/data-quality/{respondent}")
def get_data_quality(
    respondent: str,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    validate_dates(start_date, end_date)
    validate_respondent(respondent)

    freshness = run_query(
        """
        SELECT
            respondent,
            respondent_name,
            measurement_type,
            latest_period,
            database_latest_period,
            lag_hours,
            total_records,
            freshness_status
        FROM vw_data_freshness
        WHERE respondent = %s
        ORDER BY measurement_type
        """,
        (respondent,),
    )

    coverage = run_query(
        """
        SELECT
            measurement_date,
            respondent,
            respondent_name,
            demand_hours,
            forecast_hours,
            generation_hours,
            interchange_hours,
            total_measurements
        FROM vw_measurement_coverage
        WHERE respondent = %s
          AND (
              %s::date IS NULL
              OR measurement_date >= %s::date
          )
          AND (
              %s::date IS NULL
              OR measurement_date <= %s::date
          )
        ORDER BY measurement_date DESC
        LIMIT %s OFFSET %s
        """,
        (
            respondent,
            start_date,
            start_date,
            end_date,
            end_date,
            limit,
            offset,
        ),
    )

    return {
        "respondent": respondent,
        "start_date": start_date,
        "end_date": end_date,
        "freshness": freshness,
        "coverage": {
            "count": len(coverage),
            "limit": limit,
            "offset": offset,
            "data": coverage,
        },
    }
