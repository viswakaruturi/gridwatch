
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
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
    page_title="GridWatch | Demand Analytics",
    page_icon="⚡",
    layout="wide",
)

st.title("Demand Analytics")
st.caption(
    "Electricity demand patterns, daily totals, "
    "peak events, and statistical anomalies"
)


@st.cache_data(ttl=300)
def load_daily(respondent):
    return run_query("""
        SELECT *
        FROM vw_daily_demand
        WHERE respondent = :respondent
        ORDER BY demand_date
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_hourly(respondent):
    return run_query("""
        SELECT *
        FROM vw_hourly_demand_patterns
        WHERE respondent = :respondent
        ORDER BY hour_of_day
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_peaks(respondent):
    return run_query("""
        SELECT *
        FROM vw_peak_demand_events
        WHERE respondent = :respondent
        ORDER BY demand_rank
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_weekday(respondent):
    return run_query("""
        SELECT *
        FROM vw_weekday_weekend_demand
        WHERE respondent = :respondent
        ORDER BY day_type
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_anomalies(respondent):
    return run_query("""
        SELECT *
        FROM vw_demand_anomalies
        WHERE respondent = :respondent
        ORDER BY period
    """, {"respondent": respondent})


respondent, date_range, name = render_controls("Demand")

daily = load_daily(respondent)
hourly = load_hourly(respondent)
peaks = load_peaks(respondent)
weekday = load_weekday(respondent)
anomalies = load_anomalies(respondent)

st.subheader(name)

if not anomalies.empty:
    reference = pd.to_datetime(
        anomalies["period"],
        utc=True,
    ).max()
else:
    reference = None

if reference is not None:
    daily = filter_dates(
        daily,
        "demand_date",
        date_range,
        reference,
    )

    anomalies = filter_hours(
        anomalies,
        "period",
        date_range,
        reference,
    )


st.subheader("Daily Electricity Demand")

if daily.empty:
    st.info("No daily demand observations for this range.")
else:
    daily["total_demand_mwh"] = pd.to_numeric(
        daily["total_demand_mwh"],
        errors="coerce",
    )

    daily["peak_demand_mwh"] = pd.to_numeric(
        daily["peak_demand_mwh"],
        errors="coerce",
    )

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Average Daily Total",
        f"{daily['total_demand_mwh'].mean():,.0f} MWh",
    )

    c2.metric(
        "Highest Hourly Demand",
        f"{daily['peak_demand_mwh'].max():,.0f} MWh",
    )

    c3.metric("UTC Dates Shown", len(daily))

    fig = px.line(
        daily.sort_values("demand_date"),
        x="demand_date",
        y="total_demand_mwh",
        markers=True,
        labels={
            "demand_date": "Date (UTC)",
            "total_demand_mwh": "Daily Demand (MWh)",
        },
    )

    fig.update_layout(
        height=390,
        hovermode="x unified",
    )

    st.plotly_chart(fig, width="stretch")

    if (daily["reported_hours"] < 24).any():
        st.warning(
            "Some days have fewer than 24 reported demand hours."
        )

    st.caption(
        "Daily totals use UTC calendar dates. The 24-hour "
        "filter displays the latest UTC date. "
        "Partial dates can have smaller totals."
    )


st.divider()
st.subheader("Hourly Demand Patterns")

if hourly.empty:
    st.info("No hourly patterns available.")
else:
    avg = next(
        (
            column
            for column in [
                "average_demand_mwh",
                "avg_demand_mwh",
                "average_demand",
                "avg_demand",
            ]
            if column in hourly.columns
        ),
        None,
    )

    if avg:
        fig = px.bar(
            hourly,
            x="hour_of_day",
            y=avg,
            labels={
                "hour_of_day": "Hour of Day (UTC)",
                avg: "Average Hourly Demand (MWh)",
            },
        )

        fig.update_layout(
            height=390,
            xaxis=dict(dtick=1),
        )

        st.plotly_chart(fig, width="stretch")
    else:
        readable_table(hourly)

    st.caption(
        "Hourly averages use all available history, "
        "regardless of the selected date range."
    )


st.divider()
st.subheader("Peak Demand Events")

if peaks.empty:
    st.info("No peak events available.")
else:
    highest = pd.to_numeric(
        peaks["demand_mwh"],
        errors="coerce",
    ).max()

    st.metric(
        "Highest Recorded Hourly Demand",
        f"{highest:,.0f} MWh",
    )

    readable_table(
        peaks,
        rename={
            "respondent": "Code",
            "respondent_name": "Respondent",
            "period": "Timestamp",
            "demand_mwh": "Demand (MWh)",
            "demand_rank": "Rank",
        },
        timestamps=["period"],
    )

    st.caption(
        "Five highest hourly demand observations "
        "across all available history."
    )


st.divider()
st.subheader("Weekday vs Weekend Demand")

if weekday.empty:
    st.info("No weekday/weekend comparison available.")
else:
    fig = px.bar(
        weekday,
        x="day_type",
        y="average_demand_mwh",
        color="day_type",
        text="average_demand_mwh",
        labels={
            "day_type": "Day Type",
            "average_demand_mwh": "Average Hourly Demand (MWh)",
        },
    )

    fig.update_traces(
        texttemplate="%{text:,.2f}",
        textposition="outside",
    )

    fig.update_layout(
        height=380,
        showlegend=False,
        yaxis=dict(rangemode="tozero"),
    )

    st.plotly_chart(fig, width="stretch")

    readable_table(
        weekday,
        rename={
            "respondent": "Code",
            "respondent_name": "Respondent",
            "day_type": "Day Type",
            "average_demand_mwh": "Average (MWh)",
            "minimum_demand_mwh": "Minimum (MWh)",
            "maximum_demand_mwh": "Maximum (MWh)",
            "observation_count": "Hours Observed",
        },
    )

    st.caption(
        "Weekday/weekend summaries use all available "
        "history and UTC calendar days."
    )


st.divider()
st.subheader("Demand Anomaly Detection")

if anomalies.empty:
    st.info("No demand records for this range.")
else:
    anomalies = anomalies.sort_values("period").copy()

    anomalies["demand_mwh"] = pd.to_numeric(
        anomalies["demand_mwh"],
        errors="coerce",
    )

    anomalies["z_score"] = pd.to_numeric(
        anomalies["z_score"],
        errors="coerce",
    )

    flagged = anomalies[
        anomalies["anomaly_type"].isin(
            ["High Demand", "Low Demand"]
        )
    ].copy()

    high_count = (
        anomalies["anomaly_type"] == "High Demand"
    ).sum()

    low_count = (
        anomalies["anomaly_type"] == "Low Demand"
    ).sum()

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Demand Observations",
        f"{len(anomalies):,}",
    )

    c2.metric(
        "High Demand Anomalies",
        f"{high_count:,}",
    )

    c3.metric(
        "Low Demand Anomalies",
        f"{low_count:,}",
    )

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=anomalies["period"],
        y=anomalies["demand_mwh"],
        name="Actual Demand",
        mode="lines",
    ))

    for anomaly_type, color in [
        ("High Demand", "red"),
        ("Low Demand", "orange"),
    ]:
        subset = flagged[
            flagged["anomaly_type"] == anomaly_type
        ]

        fig.add_trace(go.Scatter(
            x=subset["period"],
            y=subset["demand_mwh"],
            name=anomaly_type,
            mode="markers",
            marker=dict(size=11, color=color),
            customdata=subset[["z_score"]],
            hovertemplate=(
                "Time: %{x}<br>"
                "Demand: %{y:,.0f} MWh<br>"
                "Z-score: %{customdata[0]:.2f}"
                "<extra>%{fullData.name}</extra>"
            ),
        ))

    fig.update_layout(
        xaxis_title="Time (UTC)",
        yaxis_title="Demand (MWh)",
        height=430,
        hovermode="closest",
    )

    st.plotly_chart(fig, width="stretch")

    st.caption(
        "Anomalies use z-scores relative to the "
        "respondent's usual UTC hour across all available "
        "history. They do not automatically indicate a grid fault."
    )

    st.subheader("Flagged Anomaly Records")

    if flagged.empty:
        st.info("No anomalous observations for this selection.")
    else:
        readable_table(
            flagged.sort_values(
                "period",
                ascending=False,
            ),
            rename={
                "respondent": "Code",
                "respondent_name": "Respondent",
                "period": "Timestamp",
                "demand_mwh": "Demand (MWh)",
                "mean_demand_mwh": "Hourly Baseline (MWh)",
                "stddev_demand_mwh": "Std. Deviation (MWh)",
                "z_score": "Z-score",
                "anomaly_type": "Classification",
            },
            timestamps=["period"],
        )

st.caption(
    "Source: U.S. Energy Information Administration (EIA). "
    "Analytics calculated in PostgreSQL."
)
