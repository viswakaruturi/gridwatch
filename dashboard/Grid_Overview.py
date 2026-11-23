
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db import run_query
from ui import (
    filter_hours,
    fmt_mwh,
    readable_table,
    render_controls,
)


st.set_page_config(
    page_title="GridWatch | Grid Overview",
    page_icon="⚡",
    layout="wide",
)

st.title("⚡ GridWatch")
st.caption("U.S. Electricity Grid Monitoring & Analytics")


@st.cache_data(ttl=300)
def load_snapshot(respondent):
    return run_query("""
        SELECT
            respondent,
            respondent_name,
            period,
            demand_mwh,
            generation_mwh,
            interchange_mwh,
            forecast_demand_mwh,
            balance_residual_mwh
        FROM vw_latest_grid_snapshot
        WHERE respondent = :respondent
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_demand(respondent):
    return run_query("""
        SELECT
            period,
            demand_mwh,
            rolling_24h_avg_mwh
        FROM vw_demand_trends
        WHERE respondent = :respondent
        ORDER BY period
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_forecast(respondent):
    return run_query("""
        SELECT
            period,
            actual_demand,
            forecast_demand
        FROM vw_forecast_vs_actual
        WHERE respondent = :respondent
        ORDER BY period
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_freshness(respondent):
    return run_query("""
        SELECT
            measurement_type,
            latest_period,
            lag_hours,
            freshness_status
        FROM vw_data_freshness
        WHERE respondent = :respondent
        ORDER BY measurement_type
    """, {"respondent": respondent})


respondent, date_range, name = render_controls("Grid")

snapshot_df = load_snapshot(respondent)

if snapshot_df.empty:
    st.info(
        "This respondent has no demand snapshot. "
        "Grid Operations may still contain generation "
        "or interchange measurements."
    )
    st.stop()

snapshot = snapshot_df.iloc[0]
latest = pd.to_datetime(snapshot["period"], utc=True)

demand_df = filter_hours(
    load_demand(respondent),
    "period",
    date_range,
    latest,
)

forecast_df = filter_hours(
    load_forecast(respondent),
    "period",
    date_range,
    latest,
)

freshness_df = load_freshness(respondent)

st.subheader(name)

st.caption(
    "Latest demand observation: "
    f"{latest.strftime('%B %d, %Y at %H:%M UTC')}"
)

actual = snapshot["demand_mwh"]
forecast = snapshot["forecast_demand_mwh"]

if pd.notna(actual) and pd.notna(forecast):
    error = float(actual) - float(forecast)
else:
    error = None

ops = freshness_df[
    freshness_df["measurement_type"].isin(["NG", "TI"])
]

if len(ops) < 2:
    ops_status = "Unavailable"
elif (ops["freshness_status"] == "Stale").any():
    ops_status = "Stale"
elif (ops["freshness_status"] == "Delayed").any():
    ops_status = "Delayed"
elif (ops["freshness_status"] == "Current").all():
    ops_status = "Current"
else:
    ops_status = "Unavailable"

c1, c2, c3, c4 = st.columns(4)

c1.metric("Latest Demand", fmt_mwh(actual))
c2.metric("Demand Forecast", fmt_mwh(forecast))
c3.metric("Forecast Error", fmt_mwh(error, signed=True))
c4.metric("NG / TI Reporting", ops_status)


st.divider()
st.subheader("Hourly Electricity Demand")

if demand_df.empty:
    st.info("No hourly demand available for this range.")
else:
    demand_df = demand_df.sort_values("period")

    fig = go.Figure()

    for column, label, dash in [
        ("demand_mwh", "Actual Demand", "solid"),
        (
            "rolling_24h_avg_mwh",
            "24-Hour Rolling Average",
            "dot",
        ),
    ]:
        fig.add_trace(go.Scatter(
            x=demand_df["period"],
            y=pd.to_numeric(
                demand_df[column],
                errors="coerce",
            ),
            name=label,
            mode="lines",
            line=dict(width=2, dash=dash),
        ))

    fig.update_layout(
        xaxis_title="Time (UTC)",
        yaxis_title="Demand (MWh)",
        hovermode="x unified",
        height=400,
        legend=dict(orientation="h", y=1.15),
        margin=dict(l=10, r=10, t=20, b=20),
    )

    st.plotly_chart(fig, use_container_width=True)


st.divider()
st.subheader("Actual vs Forecast Demand")

if forecast_df.empty:
    st.info("No matched forecasts available for this range.")
else:
    forecast_df = forecast_df.sort_values("period")

    fig = go.Figure()

    for column, label, dash in [
        ("actual_demand", "Actual Demand", "solid"),
        ("forecast_demand", "Forecast Demand", "dash"),
    ]:
        fig.add_trace(go.Scatter(
            x=forecast_df["period"],
            y=pd.to_numeric(
                forecast_df[column],
                errors="coerce",
            ),
            name=label,
            mode="lines",
            line=dict(width=2, dash=dash),
        ))

    fig.update_layout(
        xaxis_title="Time (UTC)",
        yaxis_title="Demand (MWh)",
        hovermode="x unified",
        height=400,
        legend=dict(orientation="h", y=1.15),
        margin=dict(l=10, r=10, t=20, b=20),
    )

    st.plotly_chart(fig, use_container_width=True)


st.divider()
st.subheader("Measurement Freshness")

if not freshness_df.empty:
    readable_table(
        freshness_df,
        rename={
            "measurement_type": "Measurement",
            "latest_period": "Latest Reported Hour",
            "lag_hours": "Lag (Hours)",
            "freshness_status": "Status",
        },
        timestamps=["latest_period"],
    )

    if ops_status in ("Delayed", "Stale"):
        st.warning(
            "Generation/interchange data is behind the "
            "latest database measurement. This can reflect "
            "EIA publication delays."
        )

st.caption(
    "Lag is relative to the freshest timestamp in PostgreSQL, "
    "not the current clock time."
)


st.divider()
st.subheader("Latest Grid Measurements")

readable_table(
    snapshot_df,
    rename={
        "respondent": "Code",
        "respondent_name": "Respondent",
        "period": "Demand Timestamp",
        "demand_mwh": "Demand (MWh)",
        "generation_mwh": "Generation (MWh)",
        "interchange_mwh": "Interchange (MWh)",
        "forecast_demand_mwh": "Forecast (MWh)",
        "balance_residual_mwh": "Balance Residual (MWh)",
    },
    timestamps=["period"],
    missing_as_text=True,
)

st.caption(
    "Unavailable values are not substituted with zero. "
    "Source: U.S. Energy Information Administration (EIA)."
)
