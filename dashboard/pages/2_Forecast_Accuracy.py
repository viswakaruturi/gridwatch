
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
    page_title="GridWatch | Forecast Accuracy",
    page_icon="⚡",
    layout="wide",
)

st.title("Forecast Accuracy")
st.caption("Day-ahead electricity demand forecasting performance")


@st.cache_data(ttl=300)
def load_forecasts(respondent):
    return run_query("""
        SELECT *
        FROM vw_forecast_vs_actual
        WHERE respondent = :respondent
        ORDER BY period
    """, {"respondent": respondent})


@st.cache_data(ttl=300)
def load_daily(respondent):
    return run_query("""
        SELECT *
        FROM vw_forecast_error_trends
        WHERE respondent = :respondent
        ORDER BY forecast_date
    """, {"respondent": respondent})


respondent, date_range, name = render_controls("Forecast")

forecast_df = load_forecasts(respondent)
daily_df = load_daily(respondent)

st.subheader(name)

if forecast_df.empty:
    st.info(
        "No matched actual/forecast observations "
        "are available for this respondent."
    )
    st.stop()

latest = pd.to_datetime(
    forecast_df["period"],
    utc=True,
).max()

forecast_df = filter_hours(
    forecast_df,
    "period",
    date_range,
    latest,
)

daily_df = filter_dates(
    daily_df,
    "forecast_date",
    date_range,
    latest,
)

if forecast_df.empty:
    st.info(
        "No matched forecast observations for this date range."
    )
    st.stop()

for column in [
    "actual_demand",
    "forecast_demand",
    "signed_error",
    "absolute_error",
    "percentage_error",
]:
    forecast_df[column] = pd.to_numeric(
        forecast_df[column],
        errors="coerce",
    )

forecast_df = forecast_df.sort_values("period")

mae = forecast_df["absolute_error"].mean()
rmse = forecast_df["signed_error"].pow(2).mean() ** 0.5
mape = forecast_df["percentage_error"].mean()
bias = forecast_df["signed_error"].mean()


st.subheader("Forecast Performance Summary")

c1, c2, c3, c4 = st.columns(4)

c1.metric("MAE", f"{mae:,.2f} MWh")
c2.metric("RMSE", f"{rmse:,.2f} MWh")

c3.metric(
    "MAPE",
    f"{mape:,.2f}%" if pd.notna(mape) else "N/A",
)

c4.metric(
    "Mean Signed Error",
    f"{bias:+,.2f} MWh",
)

st.caption(
    f"Based on {len(forecast_df):,} matched hourly observations. "
    "Positive error means actual demand exceeded the forecast."
)


st.divider()
st.subheader("Actual vs Forecast Demand")

fig = go.Figure()

for column, label, dash in [
    ("actual_demand", "Actual Demand", "solid"),
    ("forecast_demand", "Forecast Demand", "dash"),
]:
    fig.add_trace(go.Scatter(
        x=forecast_df["period"],
        y=forecast_df[column],
        name=label,
        mode="lines",
        line=dict(width=2, dash=dash),
    ))

fig.update_layout(
    xaxis_title="Time (UTC)",
    yaxis_title="Demand (MWh)",
    hovermode="x unified",
    height=420,
    legend=dict(orientation="h", y=1.15),
)

st.plotly_chart(fig, use_container_width=True)


st.divider()
st.subheader("Daily Forecast Error Trends")

if daily_df.empty:
    st.info("No daily forecast summaries for this range.")
else:
    for column in [
        "daily_mae_mwh",
        "daily_mape_percent",
        "daily_signed_error_mwh",
        "matched_forecasts",
    ]:
        daily_df[column] = pd.to_numeric(
            daily_df[column],
            errors="coerce",
        )

    left, right = st.columns(2)

    with left:
        fig = px.line(
            daily_df,
            x="forecast_date",
            y="daily_mae_mwh",
            markers=True,
            labels={
                "forecast_date": "UTC Date",
                "daily_mae_mwh": "MAE (MWh)",
            },
            title="Daily Mean Absolute Error",
        )

        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

    with right:
        fig = px.line(
            daily_df,
            x="forecast_date",
            y="daily_mape_percent",
            markers=True,
            labels={
                "forecast_date": "UTC Date",
                "daily_mape_percent": "MAPE (%)",
            },
            title="Daily Percentage Error",
        )

        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Daily Forecast Coverage")

    fig = px.bar(
        daily_df,
        x="forecast_date",
        y="matched_forecasts",
        labels={
            "forecast_date": "UTC Date",
            "matched_forecasts": "Matched Hourly Forecasts",
        },
    )

    fig.update_layout(
        height=330,
        yaxis=dict(range=[0, 25]),
    )

    st.plotly_chart(fig, use_container_width=True)

    if (daily_df["matched_forecasts"] < 24).any():
        st.warning(
            "Some UTC days have fewer than 24 matched "
            "forecasts. Incomplete days can affect "
            "daily error comparisons."
        )

    readable_table(
        daily_df.sort_values(
            "forecast_date",
            ascending=False,
        ),
        rename={
            "forecast_date": "UTC Date",
            "daily_mae_mwh": "Daily MAE (MWh)",
            "daily_mape_percent": "Daily MAPE (%)",
            "daily_signed_error_mwh": "Daily Bias (MWh)",
            "matched_forecasts": "Matched Hours",
        },
    )

    if date_range == "Last 24 Hours":
        st.caption(
            "Daily trend and coverage refer to the latest UTC "
            "calendar date; headline KPIs use the trailing 24 hours."
        )


st.divider()
st.subheader("Forecast Error Distribution")

fig = px.histogram(
    forecast_df,
    x="signed_error",
    nbins=35,
    labels={
        "signed_error": "Signed Error (MWh)",
    },
)

fig.add_vline(
    x=0,
    line_dash="dash",
    line_color="gray",
)

fig.update_layout(
    xaxis_title="Actual − Forecast (MWh)",
    yaxis_title="Number of Observations",
    height=390,
)

st.plotly_chart(fig, use_container_width=True)

st.caption(
    "Positive errors indicate underforecasting; "
    "negative errors indicate overforecasting."
)


st.divider()
st.subheader("Hourly Forecast Error Records")

readable_table(
    forecast_df.sort_values(
        "period",
        ascending=False,
    ),
    rename={
        "respondent": "Code",
        "respondent_name": "Respondent",
        "period": "Timestamp",
        "actual_demand": "Actual (MWh)",
        "forecast_demand": "Forecast (MWh)",
        "signed_error": "Signed Error (MWh)",
        "absolute_error": "Absolute Error (MWh)",
        "percentage_error": "Absolute Error (%)",
    },
    timestamps=["period"],
    height=390,
)

st.caption(
    "Source: U.S. Energy Information Administration (EIA). "
    "Error series are derived from PostgreSQL views."
)
