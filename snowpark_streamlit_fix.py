from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


TRUE_STRINGS = {"1", "true", "t", "yes", "y", "selected", "checked"}
FALSE_STRINGS = {"0", "false", "f", "no", "n", "unselected", "unchecked", ""}


def coerce_bool_series(series: pd.Series, *, default: bool = False) -> pd.Series:
    """Return a plain bool series from Snowflake, Excel, or editor-style values."""

    def coerce_value(value: Any) -> bool:
        if value is None:
            return default
        try:
            if pd.isna(value):
                return default
        except Exception:
            pass
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return bool(value)

        text = str(value).strip().lower()
        if text in TRUE_STRINGS:
            return True
        if text in FALSE_STRINGS:
            return False
        return default

    return series.map(coerce_value).astype(bool)


def inject_global_styles() -> None:
    """
    Drop-in replacement for the Compliance Rules Streamlit app's CSS injector.

    Snowflake Streamlit runs inside a host shell that can keep dark BaseWeb
    defaults on segmented controls, input fields, date pickers, select menus,
    and popover portals. The important difference from the previous CSS is that
    menu and popover selectors are not scoped only to the app container, because
    BaseWeb often renders them at document-root level.
    """

    st.markdown(
        """
        <style>
            :root {
                --elite-ink: #0f172a;
                --elite-muted: #475569;
                --elite-subtle: #64748b;
                --elite-card: rgba(255, 255, 255, 0.88);
                --elite-line: rgba(15, 23, 42, 0.11);
                --elite-primary: #2859ff;
                --elite-accent: #0f766e;
                --elite-selected: #2563eb;
                --elite-control-bg: #ffffff;
                --elite-control-soft: #f8fafc;
                --elite-control-hover: #eff6ff;
                --elite-control-selected-bg: #dbeafe;
                --elite-control-border: rgba(15, 23, 42, 0.18);
                --elite-shadow: 0 18px 50px rgba(15, 23, 42, 0.08);
            }

            html,
            body,
            [data-testid="stAppViewContainer"],
            [data-testid="stSidebar"] {
                color-scheme: light !important;
            }

            [data-testid="stAppViewContainer"] {
                background:
                    radial-gradient(circle at top right, rgba(40, 89, 255, 0.10), transparent 25%),
                    linear-gradient(180deg, #f8fbff 0%, #f5f7fb 100%);
                color: var(--elite-ink);
            }

            [data-testid="block-container"] {
                max-width: 1680px;
                padding-top: 2rem;
                padding-bottom: 3rem;
            }

            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, #ffffff 0%, #f6f8fc 100%);
                border-right: 1px solid var(--elite-line);
            }

            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] p:not(.elite-hero p),
            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] li,
            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] h1,
            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] h2,
            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] h3,
            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] h4,
            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] h5,
            [data-testid="stAppViewContainer"] [data-testid="stMarkdownContainer"] h6,
            [data-testid="stAppViewContainer"] label,
            [data-testid="stAppViewContainer"] [data-testid="stWidgetLabel"] p {
                color: var(--elite-ink);
            }

            [data-testid="stCaptionContainer"],
            [data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] {
                color: var(--elite-muted);
            }

            .elite-hero {
                position: relative;
                overflow: hidden;
                padding: 1.65rem 1.8rem;
                border-radius: 18px;
                color: #ffffff;
                background: linear-gradient(135deg, #102a56 0%, #2859ff 58%, #0f766e 100%);
                box-shadow: 0 24px 60px rgba(37, 99, 235, 0.20);
            }

            .elite-hero,
            .elite-hero *,
            .elite-hero h1,
            .elite-hero p,
            .elite-hero .elite-kicker,
            .elite-hero .elite-chip {
                color: #ffffff !important;
            }

            .elite-kicker {
                margin-bottom: 0.65rem;
                font-size: 0.78rem;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                font-weight: 700;
                opacity: 0.94;
            }

            .elite-hero h1 {
                margin: 0 0 0.45rem 0;
                font-size: 2.05rem;
                line-height: 1.08;
                letter-spacing: 0;
            }

            .elite-hero p {
                margin: 0;
                max-width: 68ch;
                font-size: 1rem;
                line-height: 1.58;
                color: rgba(255,255,255,0.91) !important;
            }

            .elite-chip-row {
                margin-top: 1rem;
                display: flex;
                gap: 0.55rem;
                flex-wrap: wrap;
            }

            .elite-chip {
                display: inline-flex;
                align-items: center;
                gap: 0.35rem;
                border: 1px solid rgba(255,255,255,0.20);
                background: rgba(255,255,255,0.14);
                padding: 0.42rem 0.72rem;
                border-radius: 999px;
                font-size: 0.85rem;
            }

            .elite-ribbon {
                display: flex;
                flex-wrap: wrap;
                gap: 0.55rem;
                margin: 0.25rem 0 1rem 0;
            }

            .elite-ribbon span {
                display: inline-flex;
                align-items: center;
                padding: 0.42rem 0.7rem;
                border-radius: 999px;
                background: rgba(15,23,42,0.04);
                border: 1px solid rgba(15,23,42,0.08);
                font-size: 0.86rem;
                color: #334155;
            }

            .elite-empty {
                padding: 1.35rem 1.2rem;
                border-radius: 14px;
                border: 1px dashed rgba(15,23,42,0.18);
                background: rgba(255,255,255,0.72);
            }

            .elite-empty h4 {
                margin: 0 0 0.25rem 0;
                color: var(--elite-ink);
                font-size: 1rem;
            }

            .elite-empty p {
                margin: 0;
                color: var(--elite-muted);
                line-height: 1.55;
            }

            [data-testid="stMetric"] {
                background: var(--elite-card);
                border: 1px solid var(--elite-line);
                border-radius: 14px;
                box-shadow: var(--elite-shadow);
                padding: 1rem 1.15rem;
            }

            div[data-testid="stMetricLabel"] p {
                letter-spacing: 0.06em;
                text-transform: uppercase;
                font-size: 0.74rem;
                color: var(--elite-subtle);
                font-weight: 700;
            }

            div[data-testid="stMetricValue"] > div {
                font-weight: 700;
                color: var(--elite-ink);
            }

            div[data-testid="stMetricDelta"] {
                color: var(--elite-muted);
            }

            /* Buttons, popover launchers, and expanders. */
            div.stButton > button,
            div.stDownloadButton > button,
            div.stLinkButton > a,
            [data-testid="stPopover"] > div > button,
            [data-testid="stExpander"] summary {
                border-radius: 999px !important;
                font-weight: 600 !important;
                min-height: 2.6rem;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
                background: rgba(255,255,255,0.98) !important;
                border: 1px solid rgba(15,23,42,0.16) !important;
                box-shadow: none !important;
                opacity: 1 !important;
            }

            div.stButton > button *,
            div.stDownloadButton > button *,
            div.stLinkButton > a *,
            [data-testid="stPopover"] > div > button *,
            [data-testid="stExpander"] summary * {
                color: inherit !important;
                -webkit-text-fill-color: inherit !important;
                fill: currentColor !important;
                stroke: currentColor !important;
            }

            div.stButton > button[kind="primary"],
            div.stDownloadButton > button[kind="primary"] {
                background: linear-gradient(135deg, #2563eb 0%, #2859ff 100%) !important;
                color: #ffffff !important;
                -webkit-text-fill-color: #ffffff !important;
                border: none !important;
            }

            div.stButton > button[kind="primary"] *,
            div.stDownloadButton > button[kind="primary"] * {
                color: #ffffff !important;
                -webkit-text-fill-color: #ffffff !important;
                fill: currentColor !important;
                stroke: currentColor !important;
            }

            /* Form controls in the app body. */
            [data-testid="stTextInput"] input,
            [data-testid="stDateInput"] input,
            [data-testid="stNumberInput"] input,
            [data-testid="stTextArea"] textarea,
            [data-baseweb="input"] input,
            [data-baseweb="textarea"] textarea {
                background: var(--elite-control-bg) !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
                caret-color: var(--elite-primary) !important;
                border-color: transparent !important;
                opacity: 1 !important;
            }

            [data-testid="stTextInput"] input::placeholder,
            [data-testid="stDateInput"] input::placeholder,
            [data-testid="stTextArea"] textarea::placeholder,
            [data-baseweb="input"] input::placeholder {
                color: #64748b !important;
                -webkit-text-fill-color: #64748b !important;
                opacity: 1 !important;
            }

            [data-testid="stTextInput"] [data-baseweb="input"],
            [data-testid="stDateInput"] [data-baseweb="input"],
            [data-testid="stNumberInput"] [data-baseweb="input"],
            [data-testid="stTextArea"] [data-baseweb="textarea"],
            [data-baseweb="select"] > div {
                background: var(--elite-control-bg) !important;
                border: 1px solid var(--elite-control-border) !important;
                border-radius: 12px !important;
                box-shadow: none !important;
                color: var(--elite-ink) !important;
                opacity: 1 !important;
            }

            [data-baseweb="select"],
            [data-baseweb="select"] *,
            [data-baseweb="tag"],
            [data-baseweb="tag"] * {
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
                fill: currentColor !important;
                stroke: currentColor !important;
            }

            [data-baseweb="tag"] {
                background: #e0f2fe !important;
                border: 1px solid #bae6fd !important;
            }

            /* Segmented controls. Streamlit/Snowflake can render these as labels
               instead of buttons, so cover labels, radios, and button variants. */
            [data-testid="stSegmentedControl"],
            [data-testid="stSegmentedControl"] > div,
            [data-testid="stSegmentedControl"] [role="radiogroup"],
            [data-testid="stSegmentedControl"] [data-baseweb="button-group"] {
                background: transparent !important;
                border: none !important;
                box-shadow: none !important;
                padding: 0 !important;
                gap: 0.3rem !important;
            }

            [data-testid="stSegmentedControl"] [role="radiogroup"] {
                display: flex !important;
                flex-wrap: wrap !important;
            }

            [data-testid="stSegmentedControl"] label,
            [data-testid="stSegmentedControl"] button,
            [data-testid="stSegmentedControl"] [role="radio"],
            [data-testid="stSegmentedControl"] [data-baseweb="radio"] {
                min-height: 2.25rem !important;
                border-radius: 999px !important;
                border: 1px solid var(--elite-control-border) !important;
                background: var(--elite-control-bg) !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
                box-shadow: none !important;
                opacity: 1 !important;
                filter: none !important;
            }

            [data-testid="stSegmentedControl"] label *,
            [data-testid="stSegmentedControl"] button *,
            [data-testid="stSegmentedControl"] [role="radio"] *,
            [data-testid="stSegmentedControl"] [data-baseweb="radio"] * {
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
                fill: currentColor !important;
                stroke: currentColor !important;
                opacity: 1 !important;
            }

            [data-testid="stSegmentedControl"] label:hover,
            [data-testid="stSegmentedControl"] button:hover,
            [data-testid="stSegmentedControl"] [role="radio"]:hover,
            [data-testid="stSegmentedControl"] [data-baseweb="radio"]:hover {
                background: var(--elite-control-hover) !important;
                border-color: rgba(37, 99, 235, 0.45) !important;
            }

            [data-testid="stSegmentedControl"] label:has(input:checked),
            [data-testid="stSegmentedControl"] button[aria-pressed="true"],
            [data-testid="stSegmentedControl"] button[aria-selected="true"],
            [data-testid="stSegmentedControl"] [role="radio"][aria-checked="true"],
            [data-testid="stSegmentedControl"] [data-baseweb="radio"][aria-checked="true"] {
                background: var(--elite-control-selected-bg) !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
                border-color: var(--elite-selected) !important;
                box-shadow: inset 0 0 0 1px var(--elite-selected) !important;
            }

            /* BaseWeb portals: select dropdowns, multiselect menus, datepicker menus,
               and Streamlit popovers can be mounted outside stAppViewContainer. */
            [data-baseweb="popover"],
            [data-baseweb="popover"] *,
            [data-baseweb="menu"],
            [data-baseweb="menu"] *,
            [data-baseweb="datepicker"],
            [data-baseweb="datepicker"] *,
            [data-baseweb="calendar"],
            [data-baseweb="calendar"] *,
            [role="listbox"],
            [role="listbox"] *,
            [role="menu"],
            [role="menu"] * {
                color-scheme: light !important;
            }

            [data-baseweb="popover"] > div,
            [data-baseweb="menu"],
            [data-baseweb="datepicker"],
            [data-baseweb="calendar"],
            [role="listbox"],
            [role="menu"] {
                background: #ffffff !important;
                color: var(--elite-ink) !important;
                border: 1px solid rgba(15, 23, 42, 0.14) !important;
                border-radius: 12px !important;
                box-shadow: 0 18px 42px rgba(15, 23, 42, 0.14) !important;
            }

            [role="option"],
            [role="menuitem"],
            [data-baseweb="menu"] li,
            [data-baseweb="menu"] div {
                background: #ffffff !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
            }

            [role="option"]:hover,
            [role="menuitem"]:hover,
            [data-baseweb="menu"] li:hover {
                background: var(--elite-control-hover) !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
            }

            [role="option"][aria-selected="true"],
            [role="option"][aria-checked="true"],
            [role="menuitem"][aria-selected="true"],
            [data-baseweb="menu"] li[aria-selected="true"] {
                background: var(--elite-control-selected-bg) !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
            }

            [data-baseweb="calendar"] button,
            [data-baseweb="calendar"] [role="gridcell"],
            [data-baseweb="datepicker"] button {
                background: #ffffff !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
            }

            [data-baseweb="calendar"] button:hover,
            [data-baseweb="datepicker"] button:hover {
                background: var(--elite-control-hover) !important;
            }

            [data-testid="stFileUploader"] section {
                background: rgba(255,255,255,0.86) !important;
                border: 1px dashed rgba(15,23,42,0.22) !important;
                border-radius: 14px !important;
                color: var(--elite-ink) !important;
            }

            [data-testid="stFileUploader"] section *,
            [data-testid="stFileUploader"] label * {
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
            }

            [data-testid="stTabs"] [data-baseweb="tab-list"] {
                gap: 0.35rem;
                background: transparent !important;
                border: none !important;
                box-shadow: none !important;
            }

            [data-testid="stTabs"] [data-baseweb="tab"] {
                border-radius: 999px;
                padding: 0.58rem 0.9rem;
                border: 1px solid rgba(15,23,42,0.12) !important;
                background: #ffffff !important;
                color: var(--elite-ink) !important;
                -webkit-text-fill-color: var(--elite-ink) !important;
            }

            [data-testid="stTabs"] [data-baseweb="tab"] *,
            [data-testid="stTabs"] [data-baseweb="tab"] svg,
            [data-testid="stTabs"] [data-baseweb="tab"] svg * {
                color: inherit !important;
                -webkit-text-fill-color: inherit !important;
                fill: currentColor !important;
                stroke: currentColor !important;
            }

            [data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] {
                background: var(--elite-control-selected-bg) !important;
                border-color: var(--elite-selected) !important;
                box-shadow: inset 0 0 0 1px var(--elite-selected) !important;
            }

            [data-testid="stTable"] table,
            [data-testid="stTable"] th,
            [data-testid="stTable"] td {
                color: var(--elite-ink) !important;
                background: rgba(255,255,255,0.78) !important;
            }

            [data-testid="stTable"] th {
                font-weight: 700;
            }

            [data-testid="stDataFrame"],
            [data-testid="stTable"] {
                border-radius: 14px;
                overflow: hidden;
                border: 1px solid rgba(15, 23, 42, 0.10);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
