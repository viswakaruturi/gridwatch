
from datetime import timedelta

import pandas as pd
import streamlit as st

from db import run_query


DATE_OPTIONS = [
    "Last 24 Hours",
    "Last 7 Days",
    "Last 14 Days",
    "All Available",
]

MEASUREMENT_LABELS = {
    "D": "Demand",
    "DF": "Demand Forecast",
    "NG": "Net Generation",
    "TI": "Total Interchange",
}


@st.cache_data(ttl=300)
def load_respondents():
    return run_query("""
        SELECT
            respondent,
            MAX(respondent_name) AS respondent_name
        FROM electricity
        GROUP BY respondent
        ORDER BY respondent_name
    """)


def _remember_authority():
    st.session_state["gw_authority"] = (
        st.session_state["_gw_authority_widget"]
    )


def _remember_range():
    st.session_state["gw_date_range"] = (
        st.session_state["_gw_range_widget"]
    )


def render_controls(section="Grid"):
    respondents = load_respondents()

    if respondents.empty:
        st.error(
            "No electricity measurements are available in PostgreSQL."
        )
        st.stop()

    labels = dict(zip(
        respondents["respondent"],
        respondents["respondent_name"],
    ))

    codes = respondents["respondent"].tolist()

    selected = st.session_state.get("gw_authority", "TEPC")

    if selected not in codes:
        selected = "TEPC" if "TEPC" in codes else codes[0]

    st.session_state["gw_authority"] = selected
    st.session_state["_gw_authority_widget"] = selected

    selected_range = st.session_state.get(
        "gw_date_range",
        "Last 7 Days",
    )

    if selected_range not in DATE_OPTIONS:
        selected_range = "Last 7 Days"

    st.session_state["gw_date_range"] = selected_range
    st.session_state["_gw_range_widget"] = selected_range

    st.sidebar.header(f"{section} Controls")

    st.sidebar.selectbox(
        "Balancing Authority / Respondent",
        options=codes,
        format_func=lambda code: f"{labels[code]} ({code})",
        key="_gw_authority_widget",
        on_change=_remember_authority,
    )

    st.sidebar.selectbox(
        "Date Range",
        options=DATE_OPTIONS,
        key="_gw_range_widget",
        on_change=_remember_range,
    )

    st.sidebar.caption("Dates and timestamps use UTC.")

    if st.sidebar.button("Refresh Data", key="gw_refresh"):
        st.cache_data.clear()
        st.rerun()

    return (
        st.session_state["gw_authority"],
        st.session_state["gw_date_range"],
        labels[st.session_state["gw_authority"]],
    )


def filter_hours(
    df,
    timestamp_column,
    selected_range,
    reference_timestamp=None,
):
    if df.empty or selected_range == "All Available":
        return df.copy()

    result = df.copy()

    result[timestamp_column] = pd.to_datetime(
        result[timestamp_column],
        utc=True,
    )

    if reference_timestamp is not None:
        reference = pd.to_datetime(
            reference_timestamp,
            utc=True,
        )
    else:
        reference = result[timestamp_column].max()

    hours = {
        "Last 24 Hours": 24,
        "Last 7 Days": 168,
        "Last 14 Days": 336,
    }[selected_range]

    cutoff = reference - pd.Timedelta(hours=hours - 1)

    return result[
        result[timestamp_column] >= cutoff
    ].copy()


def filter_dates(
    df,
    date_column,
    selected_range,
    reference_date=None,
):
    if df.empty or selected_range == "All Available":
        return df.copy()

    result = df.copy()

    result[date_column] = pd.to_datetime(
        result[date_column]
    ).dt.date

    if reference_date is not None:
        end = pd.to_datetime(reference_date).date()
    else:
        end = max(result[date_column])

    days = {
        "Last 24 Hours": 1,
        "Last 7 Days": 7,
        "Last 14 Days": 14,
    }[selected_range]

    cutoff = end - timedelta(days=days - 1)

    return result[
        (result[date_column] >= cutoff)
        & (result[date_column] <= end)
    ].copy()


def utc_display(df, columns):
    result = df.copy()

    for column in columns:
        if column in result.columns:
            result[column] = pd.to_datetime(
                result[column],
                utc=True,
                errors="coerce",
            ).dt.strftime("%Y-%m-%d %H:%M UTC")

    return result


def readable_table(
    df,
    rename=None,
    timestamps=(),
    missing_as_text=False,
    height=None,
):
    result = utc_display(df, timestamps)

    if missing_as_text:
        result = result.astype("string").fillna(
            "Not reported at this timestamp"
        )

    if rename:
        result = result.rename(columns=rename)

    dataframe_options = {
        "hide_index": True,
        "width": "stretch",
    }

    if height is not None:
        dataframe_options["height"] = height

    st.dataframe(
        result,
        **dataframe_options,
    )


def fmt_mwh(value, signed=False, decimals=0):
    if pd.isna(value):
        return "N/A"

    spec = f"{'+ ' if signed else ''},.{decimals}f"
    spec = spec.replace("+ ", "+")

    return f"{float(value):{spec}} MWh"
