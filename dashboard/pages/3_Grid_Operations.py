
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]),
)

from db import run_query
from ui import (
    filter_dates,
    filter_hours,
    readable_table,
    render_controls,
)


st.set_page_config(
    page_title="GridWatch | Grid Operations",
    page_icon="⚡",
    layout="wide",
)

st.title("Grid Operations & Data Quality")
st.caption(
    "Generation, interchange, accounting balance, "
    "completeness, and reporting freshness"
)


@st.cache_data(ttl=300)
def load_balance(respondent):
    return run_query("""
        SELECT *
        FROM vw_generation_demand_balance
        WHERE respondent = :respondent
        ORDER BY period
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_interchange(respondent):
    return run_query("""
        SELECT *
        FROM vw_interchange_patterns
        WHERE respondent = :respondent
        ORDER BY period
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_coverage(respondent):
    return run_query("""
        SELECT *
        FROM vw_measurement_coverage
        WHERE respondent = :respondent
        ORDER BY measurement_date
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_missing(respondent):
    return run_query("""
        SELECT *
        FROM vw_missing_hourly_records
        WHERE respondent = :respondent
        ORDER BY missing_period DESC
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_freshness(respondent):
    return run_query("""
        SELECT *
        FROM vw_data_freshness
        WHERE respondent = :respondent
        ORDER BY measurement_type
    """, {"respondent": respondent})


respondent, date_range, name = render_controls("Operations")

balance_df = load_balance(respondent)
interchange_df = load_interchange(respondent)
coverage_df = load_coverage(respondent)
missing_df = load_missing(respondent)
freshness_df = load_freshness(respondent)

st.subheader(name)

reference_candidates = []

if not balance_df.empty:
    reference_candidates.append(
        pd.to_datetime(
            balance_df["period"],
            utc=True,
        ).max()
    )

if not interchange_df.empty:
    reference_candidates.append(
        pd.to_datetime(
            interchange_df["period"],
            utc=True,
        ).max()
    )

if not coverage_df.empty:
    reference_candidates.append(
        pd.to_datetime(
            coverage_df["measurement_date"],
            utc=True,
        ).max()
    )

reference = (
    max(reference_candidates)
    if reference_candidates
    else None
)

if reference is not None:
    balance_df = filter_hours(
        balance_df,
        "period",
        date_range,
        reference,
    )

    interchange_df = filter_hours(
        interchange_df,
        "period",
        date_range,
        reference,
    )

    coverage_df = filter_dates(
        coverage_df,
        "measurement_date",
        date_range,
        reference,
    )

    missing_df = filter_hours(
        missing_df,
        "missing_period",
        date_range,
        reference,
    )


st.subheader("Measurement Freshness")

if freshness_df.empty:
    st.info("No measurement freshness records available.")
else:
    readable_table(
        freshness_df,
        rename={
            "respondent": "Code",
            "respondent_name": "Respondent",
            "measurement_type": "Measurement",
            "latest_period": "Latest Reported Hour",
            "database_latest_period": "Database Latest Hour",
            "lag_hours": "Lag (Hours)",
            "total_records": "Historical Records",
            "freshness_status": "Status",
        },
        timestamps=[
            "latest_period",
            "database_latest_period",
        ],
    )

    if freshness_df["freshness_status"].isin(
        ["Delayed", "Stale"]
    ).any():
        st.warning(
            "Some measurements lag behind the freshest "
            "records in PostgreSQL. This can reflect "
            "EIA publication delays."
        )

    st.caption(
        "Lag is relative to the freshest database timestamp, "
        "not the current real-world time."
    )


st.divider()
st.subheader("Generation and Demand Balance")

if balance_df.empty:
    st.info(
        "No demand-based balance records are available "
        "for this range."
    )
else:
    for column in [
        "demand_mwh",
        "generation_mwh",
        "interchange_mwh",
        "generation_minus_demand_mwh",
        "balance_residual_mwh",
    ]:
        balance_df[column] = pd.to_numeric(
            balance_df[column],
            errors="coerce",
        )

    complete = balance_df.dropna(
        subset=[
            "demand_mwh",
            "generation_mwh",
            "interchange_mwh",
        ]
    ).sort_values("period")

    total_hours = len(balance_df)

    c1, c2, c3 = st.columns(3)

    c1.metric("Demand Hours", f"{total_hours:,}")

    c2.metric(
        "Complete Balance Hours",
        f"{len(complete):,}",
    )

    c3.metric(
        "Complete Coverage",
        f"{len(complete) / total_hours * 100:.1f}%",
    )

    if not complete.empty:
        fig = go.Figure()

        for column, label in [
            ("demand_mwh", "Demand"),
            ("generation_mwh", "Generation"),
        ]:
            fig.add_trace(go.Scatter(
                x=complete["period"],
                y=complete[column],
                name=label,
                mode="lines",
            ))

        fig.update_layout(
            xaxis_title="Time (UTC)",
            yaxis_title="Electricity (MWh)",
            hovermode="x unified",
            height=400,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )

        st.metric(
            "Average Absolute Balance Residual",
            f"{complete['balance_residual_mwh'].abs().mean():,.2f} MWh",
        )
    else:
        st.info(
            "No hours have all three measurements in this range."
        )

    st.caption(
        "Residual = Generation − Interchange − Demand. "
        "Nonzero residuals can reflect reporting/accounting differences."
    )

    with st.expander("Hourly Balance Records"):
        readable_table(
            balance_df.sort_values(
                "period",
                ascending=False,
            ),
            rename={
                "respondent": "Code",
                "respondent_name": "Respondent",
                "period": "Timestamp",
                "demand_mwh": "Demand (MWh)",
                "generation_mwh": "Generation (MWh)",
                "interchange_mwh": "Interchange (MWh)",
                "generation_minus_demand_mwh": (
                    "Generation − Demand (MWh)"
                ),
                "balance_residual_mwh": (
                    "Balance Residual (MWh)"
                ),
            },
            timestamps=["period"],
            missing_as_text=True,
            height=350,
        )


st.divider()
st.subheader("Electricity Imports and Exports")

if interchange_df.empty:
    st.info("No interchange observations for this range.")
else:
    for column in [
        "interchange_mwh",
        "rolling_24h_avg_mwh",
    ]:
        interchange_df[column] = pd.to_numeric(
            interchange_df[column],
            errors="coerce",
        )

    interchange_df = interchange_df.sort_values("period")

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Net Export Hours",
        f"{(interchange_df['flow_direction'] == 'Net Export').sum():,}",
    )

    c2.metric(
        "Net Import Hours",
        f"{(interchange_df['flow_direction'] == 'Net Import').sum():,}",
    )

    c3.metric(
        "Balanced Hours",
        f"{(interchange_df['flow_direction'] == 'Balanced').sum():,}",
    )

    fig = go.Figure()

    for column, label, dash in [
        ("interchange_mwh", "Net Interchange", "solid"),
        (
            "rolling_24h_avg_mwh",
            "24-Hour Rolling Average",
            "dash",
        ),
    ]:
        fig.add_trace(go.Scatter(
            x=interchange_df["period"],
            y=interchange_df[column],
            name=label,
            mode="lines",
            line=dict(width=2, dash=dash),
        ))

    fig.add_hline(
        y=0,
        line_color="gray",
        line_dash="dot",
    )

    fig.update_layout(
        xaxis_title="Time (UTC)",
        yaxis_title="Net Interchange (MWh)",
        hovermode="x unified",
        height=400,
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    st.caption(
        "Positive interchange means net exports; "
        "negative interchange means net imports."
    )

    with st.expander("Hourly Interchange Records"):
        readable_table(
            interchange_df.sort_values(
                "period",
                ascending=False,
            ),
            rename={
                "respondent": "Code",
                "respondent_name": "Respondent",
                "period": "Timestamp",
                "interchange_mwh": "Interchange (MWh)",
                "flow_direction": "Direction",
                "previous_interchange_mwh": (
                    "Previous Hour (MWh)"
                ),
                "interchange_change_mwh": (
                    "Hourly Change (MWh)"
                ),
                "rolling_24h_avg_mwh": (
                    "24-Hour Mean (MWh)"
                ),
            },
            timestamps=["period"],
            missing_as_text=True,
            height=350,
        )


st.divider()
st.subheader("Daily Measurement Coverage")

if coverage_df.empty:
    st.info(
        "No measurement coverage records for this range."
    )
else:
    fig = go.Figure()

    for column, label in [
        ("demand_hours", "Demand"),
        ("forecast_hours", "Forecast"),
        ("generation_hours", "Generation"),
        ("interchange_hours", "Interchange"),
    ]:
        fig.add_trace(go.Scatter(
            x=coverage_df["measurement_date"],
            y=coverage_df[column],
            name=label,
            mode="lines+markers",
        ))

    fig.add_hline(
        y=24,
        line_dash="dash",
        line_color="gray",
    )

    fig.update_layout(
        xaxis_title="Date (UTC)",
        yaxis_title="Reported Hours",
        hovermode="x unified",
        height=400,
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )

    readable_table(
        coverage_df.sort_values(
            "measurement_date",
            ascending=False,
        ),
        rename={
            "respondent": "Code",
            "respondent_name": "Respondent",
            "measurement_date": "UTC Date",
            "demand_hours": "Demand Hours",
            "forecast_hours": "Forecast Hours",
            "generation_hours": "Generation Hours",
            "interchange_hours": "Interchange Hours",
            "total_measurements": "Total Measurements",
        },
    )

    st.caption(
        "24 hourly observations indicate a full UTC day for "
        "a type the respondent reports. Trailing EIA reporting "
        "delays can reduce counts."
    )


st.divider()
st.subheader("Missing Hourly Records")

if missing_df.empty:
    st.info(
        "No internal hourly gaps detected for this "
        "respondent and date range."
    )
else:
    missing_summary = (
        missing_df.groupby("measurement_type")
        .size()
        .reset_index(name="missing_hours")
    )

    st.metric(
        "Missing Internal Respondent-Hours",
        f"{len(missing_df):,}",
    )

    st.bar_chart(
        missing_summary,
        x="measurement_type",
        y="missing_hours",
    )

    readable_table(
        missing_df.sort_values(
            "missing_period",
            ascending=False,
        ),
        rename={
            "respondent": "Code",
            "respondent_name": "Respondent",
            "measurement_type": "Measurement",
            "missing_period": "Missing UTC Hour",
        },
        timestamps=["missing_period"],
    )

st.caption(
    "This checks gaps between the first and last recorded "
    "hour of each measurement type. It does not count "
    "trailing reporting delays as internal gaps."
)

st.caption(
    "Source: U.S. Energy Information Administration (EIA). "
    "Operational and quality metrics computed in PostgreSQL."
)
