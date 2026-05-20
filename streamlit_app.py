from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Mapping
import hashlib
import html
import math
import re
import uuid

import altair as alt
import pandas as pd
import streamlit as st

try:
    from snowflake.snowpark.context import get_active_session
except Exception:  # Allows local preview outside Snowflake.
    get_active_session = None

from snowpark_streamlit_fix import coerce_bool_series, inject_global_styles


st.set_page_config(
    page_title="Analyst Rules Command Center",
    page_icon=":material/rule:",
    layout="wide",
    initial_sidebar_state="collapsed",
)


TABLE_WORKFLOW = "CLAB_PROTO_WORKFLOW_REQUEST"
TABLE_RULE_CATALOG = "CLAB_PROTO_RULE_CATALOG"
TABLE_LOCAL_VENDOR = "CLAB_PROTO_RULE_LOCAL_VENDOR_EXCLUSION"
TABLE_DISALLOWED_MIN = "CLAB_PROTO_RULE_DISALLOWED_MIN"
TABLE_ALLOWLIST = "CLAB_PROTO_RULE_APPROVED_ITEM_ALLOWLIST"

TABLE_DATABASE = ""
TABLE_SCHEMA = ""


SOURCE_COLUMNS = [
    "Business",
    "Type",
    "Case#",
    "Date Created",
    "Sector",
    "Division",
    "Unit Name",
    "Unit Number",
    "Distributor Account Number",
    "Vendor",
    "Parent",
    "DST",
    "DSTDIN",
    "Item Code",
    "Parent Category",
    "Sub Category",
    "DIN",
    "MIN",
    "Manufacturer",
    "Brand",
    "Description",
    "Supply Chain Description",
    "Pack",
    "Usage",
    "One-Time or Permanent",
    "Reason for request",
    "Buysmart Action",
    "DPL",
    "Meets Criteria",
    "In CAT",
    "On MOG",
    "Pantry",
    "K12 APL",
    "Compass APL",
    "ACTION",
    "If In Stock: Action",
    "Conversion DIN",
    "Conversion Manufacturer",
    "Conversion Mfr ID",
    "Conversion Min",
    "Conversion Ceres Catalog ID",
    "Conversion Brand",
    "Conversion Item Description",
    "Conversion Dist Item Pack Size",
    "Audit Action",
    "Conversion VA%",
]


APP_COLUMNS = [
    "workflow_request_id",
    "batch_id",
    "source_file_name",
    "source_sheet_name",
    "source_row_number",
    "reporting_date",
    "is_active",
    "Upstream Action",
    "Upstream If In Stock: Action",
    "Upstream Buysmart Action",
    "Rule Applied",
    "Needs Review",
    "Analyst Notes",
    "Validation Status",
    "Excluded",
    "Excluded Reason",
    "Queue Bucket",
    "Request Bucket",
    "Outcome Reporting",
    "Selected",
    "Last Sync",
    "Last Saved",
    "Assignment",
    "Status",
]


COLUMN_ALIASES = {
    "Business": ["business"],
    "Type": ["type", "request type"],
    "Case#": ["case#", "case #", "case_number", "case number"],
    "Date Created": ["date created", "created date"],
    "Sector": ["sector"],
    "Division": ["division"],
    "Unit Name": ["unit name"],
    "Unit Number": ["unit number"],
    "Distributor Account Number": ["distributor account number"],
    "Vendor": ["vendor"],
    "Parent": ["parent"],
    "DST": ["dst"],
    "DSTDIN": ["dstdin"],
    "Item Code": ["item code"],
    "Parent Category": ["parent category"],
    "Sub Category": ["sub category", "subcategory"],
    "DIN": ["din"],
    "MIN": ["min"],
    "Manufacturer": ["manufacturer"],
    "Brand": ["brand"],
    "Description": ["description", "item description"],
    "Supply Chain Description": ["supply chain description"],
    "Pack": ["pack"],
    "Usage": ["usage"],
    "One-Time or Permanent": ["one-time or permanent", "1x / permanent", "one time or permanent"],
    "Reason for request": ["reason for request", "reason for request ", "reason"],
    "Buysmart Action": ["buysmart action", "buy smart action"],
    "DPL": ["dpl"],
    "Meets Criteria": ["meets criteria", "va / stocking criteria", "stocking criteria"],
    "In CAT": ["in cat"],
    "On MOG": ["on mog"],
    "Pantry": ["pantry", "pantry indicator"],
    "K12 APL": ["k12 apl", "k12 indicator", "k12"],
    "Compass APL": ["compass apl", "apl indicator", "apl"],
    "ACTION": ["action"],
    "If In Stock: Action": ["if in stock: action", "if in stock action"],
    "Conversion DIN": ["conversion din"],
    "Conversion Manufacturer": ["conversion manufacturer"],
    "Conversion Mfr ID": ["conversion mfr id"],
    "Conversion Min": ["conversion min"],
    "Conversion Ceres Catalog ID": ["conversion ceres catalog id"],
    "Conversion Brand": ["conversion brand"],
    "Conversion Item Description": ["conversion item description"],
    "Conversion Dist Item Pack Size": ["conversion dist item pack size"],
    "Audit Action": ["audit action", "comments / notes", "analyst notes", "notes"],
    "Conversion VA%": ["conversion va%", "conversion va"],
}


UI_TO_DB = {
    "workflow_request_id": "WORKFLOW_REQUEST_ID",
    "batch_id": "BATCH_ID",
    "source_file_name": "SOURCE_FILE_NAME",
    "source_sheet_name": "SOURCE_SHEET_NAME",
    "source_row_number": "SOURCE_ROW_NUMBER",
    "reporting_date": "REPORTING_DATE",
    "is_active": "IS_ACTIVE",
    "Business": "BUSINESS",
    "Type": "REQUEST_TYPE",
    "Case#": "CASE_NUMBER",
    "Date Created": "DATE_CREATED",
    "Sector": "SECTOR",
    "Division": "DIVISION",
    "Unit Name": "UNIT_NAME",
    "Unit Number": "UNIT_NUMBER",
    "Distributor Account Number": "DISTRIBUTOR_ACCOUNT_NUMBER",
    "Vendor": "VENDOR",
    "Parent": "PARENT",
    "DST": "DST",
    "DSTDIN": "DSTDIN",
    "Item Code": "ITEM_CODE",
    "Parent Category": "PARENT_CATEGORY",
    "Sub Category": "SUBCATEGORY",
    "DIN": "DIN",
    "MIN": "MIN",
    "Manufacturer": "MANUFACTURER",
    "Brand": "BRAND",
    "Description": "ITEM_DESCRIPTION",
    "Supply Chain Description": "SUPPLY_CHAIN_DESCRIPTION",
    "Pack": "PACK",
    "Usage": "USAGE_QTY",
    "One-Time or Permanent": "ONE_TIME_OR_PERMANENT",
    "Reason for request": "REASON_FOR_REQUEST",
    "Buysmart Action": "BUYSMART_ACTION",
    "DPL": "DPL",
    "Meets Criteria": "MEETS_CRITERIA",
    "In CAT": "IN_CAT",
    "On MOG": "ON_MOG",
    "Pantry": "PANTRY",
    "K12 APL": "K12_APL",
    "Compass APL": "COMPASS_APL",
    "ACTION": "ACTION",
    "If In Stock: Action": "IF_IN_STOCK_ACTION",
    "Conversion DIN": "CONVERSION_DIN",
    "Conversion Manufacturer": "CONVERSION_MANUFACTURER",
    "Conversion Mfr ID": "CONVERSION_MFR_ID",
    "Conversion Min": "CONVERSION_MIN",
    "Conversion Ceres Catalog ID": "CONVERSION_CERES_CATALOG_ID",
    "Conversion Brand": "CONVERSION_BRAND",
    "Conversion Item Description": "CONVERSION_ITEM_DESCRIPTION",
    "Conversion Dist Item Pack Size": "CONVERSION_DIST_ITEM_PACK_SIZE",
    "Upstream Action": "UPSTREAM_ACTION",
    "Upstream If In Stock: Action": "UPSTREAM_IF_IN_STOCK_ACTION",
    "Upstream Buysmart Action": "UPSTREAM_BUYSMART_ACTION",
    "Conversion VA%": "CONVERSION_VA_PCT",
    "Rule Applied": "RULE_APPLIED",
    "Needs Review": "NEEDS_REVIEW",
    "Analyst Notes": "ANALYST_NOTES",
    "Validation Status": "VALIDATION_STATUS",
    "Excluded": "EXCLUDED_FLAG",
    "Excluded Reason": "EXCLUDED_REASON",
    "Queue Bucket": "QUEUE_BUCKET",
    "Request Bucket": "REQUEST_BUCKET",
    "Outcome Reporting": "OUTCOME_REPORTING",
    "Selected": "SELECTED_FLAG",
    "Last Sync": "LAST_SYNC_AT",
    "Last Saved": "LAST_SAVED_AT",
    "Assignment": "ASSIGNMENT",
    "Status": "STATUS",
}


DB_TO_UI = {db_col: ui_col for ui_col, db_col in UI_TO_DB.items()}


DEFAULT_LOCAL_VENDORS = [
    "Baldor",
    "Network",
    "UNFI",
    "Vesta",
    "Vistar Vending",
    "The Chefs Warehouse",
]


DEFAULT_DISALLOWED_HIGLINER = [
    ("1029719", "Cod Fillet Beer Battered Corona 4 Oz 1/10 Lb"),
    ("10002677", "Pollock Beer Battered IPA 2-3 Oz 1/10 Lb"),
    ("53267", "Pollock Filet Breaded Whole Grain Rectangle Oven Ready 3.6 Oz PUFI CN 1/10 Lb"),
    ("10003405", "Fish Cake 2 Oz Breaded 1/10 Lb"),
    ("26240", "Pollock Breaded Stick Whole Grain 1 Oz CN 1/10 Lb"),
    ("10022263", "Pollock Wedge Battered Dipped CN 4 Oz 1/10 Lb"),
    ("10002375", "Pollock Alaskan Scroddle Dixie Crunch CN Kosher 1.5oz 1/10 Lb"),
    ("10023828", "Pollock Filet Breaded Battered 8 Oz Big Bobs Belly Buster 1/10 Lb"),
    ("1089097", "Pollock Alaska Raw Breaded Squares 40/4 Oz 1/10 Lb (89097)"),
    ("06233", "Pollock Potato Crunch Style Whole Grain 3.6 Oz 1/20.7 Lb"),
    ("10022984", "Fish Blend Patty 4 Oz 1/10 Lb"),
    ("10001186", "Pollock Alaskan Breaded Oven Ready Rectangle CN Kosher 3 Oz 1/10 Lb"),
    ("10021672", "Cod Stick Breaded FC CN 1 Oz 1/10 Lb"),
    ("10022055", "Cod Portion Breaded Rectangle FC CN Kosher 3 Oz 1/10 Lb"),
    ("10003352", "Fish Patty Breaded Surf Burger CN 3 Oz 1/10 Lb"),
    ("06646", "Pollock Alaska Filet Precooked Southern Style Cornmeal Nordica Style Filet 3.6 Oz 1/10.35 Lb"),
    ("53364", "Pollock Breaded Wedge Potato Crunchy Oven Ready CN 46/3.6 Oz 1/10.35 Lb (506330)"),
    ("06551", "Pollock Alaska Nuggets Precooked Potato Crunch Style 1 Oz 1/10 Lb"),
]


DEFAULT_LAMB_WESTON_ALLOWLIST = [
    {"MIN": "30H", "BRAND": "Lamb Weston", "DESCRIPTION": "Potato French Fry Natural Raw 1/8 In Chip Skin On 6/5 Lb"},
    {"MIN": "L0094", "BRAND": "Sweet Things", "DESCRIPTION": "Potato Sweet Puff Mini 6/2.5 Lb"},
]


APPROVED_BRANDS_COMPASS = {"sweet streets", "evergood", "passport", "medtrition", "uproot", "european imports"}
APPROVED_BRANDS_HEALTHTRUST = {"passport", "evergood", "medtrition"}
MORRISON_BALLARD_SUBBRANDS = {"pjs coffee", "new orleans roast", "crescent city"}
SPECIAL_COMPASS_APPROVED_MANUFACTURERS = {"great lakes", "sara lee frozen", "bob's red mill"}
SPECIAL_COMPASS_APPROVED_BRANDS = {"diversey", "passport", "uproot", "evergood", "zero acres farms", "path water"}
SPECIAL_COMPASS_PRF_ONLY_BRANDS = {"soda stream"}


REQUEST_BUCKET_ORDER = [
    "Mass Add",
    "Mass SRF",
    "PRF",
    "SORF",
    "SRF",
    "Already On MOG / Check Attribute",
    "Cannot Add Not in Stock",
    "Conversion DIN / Use Right",
    "1x request",
    "Permanent request",
    "Excluded / Local DC",
    "Special exception / analyst review",
]


OUTCOME_REPORT_ORDER = [
    "approved",
    "denied",
    "1x approved",
    "use right",
    "find alt first",
    "send/check with CDM",
    "assigned",
    "unresolved exceptions",
]


RULE_SUMMARY_DEFAULT = pd.DataFrame(
    {
        "Metric": [
            "Total harvested rules",
            "Automation Candidate = Yes",
            "Automation Candidate = Partial",
            "Automation Candidate = No",
            "Alpha Recommendation = Alpha",
            "Alpha Recommendation = Guided",
            "Alpha Recommendation = Future",
        ],
        "Count": [69, 50, 18, 1, 50, 14, 5],
    }
)


WORKSPACE_OPTIONS = ["Workflow Dashboard", "Outcome Reporting", "Rule Catalog"]
WORKSPACE_META = {
    "Workflow Dashboard": {
        "nav": "Workflow",
        "kicker": "Analyst operations surface",
        "title": "Review the daily decision queue with clarity.",
        "description": "Run harvested Alpha rules, focus the exception queue, and save analyst-ready outcomes back to Snowflake.",
    },
    "Outcome Reporting": {
        "nav": "Reporting",
        "kicker": "Operating summary",
        "title": "Turn the working set into a publish-ready rollup.",
        "description": "See approved, denied, use-right, and unresolved outcomes in a cleaner stakeholder view.",
    },
    "Rule Catalog": {
        "nav": "Catalog",
        "kicker": "Automation inventory",
        "title": "Inspect the harvested rule base with confidence.",
        "description": "Measure automation coverage, browse rule details, and spot guided recommendations.",
    },
}


QUEUE_LENS_OPTIONS = ["All", "Needs Review", "Approved", "Denied", "Assigned", "Excluded"]
OUTCOME_LENS_OPTIONS = ["All", "Approved", "Denied", "Use Right / Alt", "Needs Review"]


WORKBENCH_COLUMNS = [
    "Selected",
    "Business",
    "Type",
    "Case#",
    "Division",
    "Vendor",
    "Description",
    "DIN",
    "MIN",
    "One-Time or Permanent",
    "Meets Criteria",
    "ACTION",
    "If In Stock: Action",
    "Buysmart Action",
    "Needs Review",
    "Validation Status",
    "Analyst Notes",
    "Excluded",
    "Excluded Reason",
    "Assignment",
    "Status",
    "Rule Applied",
    "Last Sync",
    "Last Saved",
]


WORKBENCH_EDITABLE_COLUMNS = {
    "Selected",
    "ACTION",
    "If In Stock: Action",
    "Buysmart Action",
    "Needs Review",
    "Validation Status",
    "Analyst Notes",
    "Excluded",
    "Excluded Reason",
    "Assignment",
    "Status",
}


REVIEW_COLUMNS = [
    "Business",
    "Type",
    "Case#",
    "Division",
    "Vendor",
    "Description",
    "ACTION",
    "Buysmart Action",
    "Needs Review",
    "Validation Status",
    "Analyst Notes",
    "Rule Applied",
    "Excluded",
    "Excluded Reason",
    "Status",
]


OUTCOME_DETAIL_COLUMNS = [
    "Business",
    "Type",
    "Case#",
    "Vendor",
    "Description",
    "One-Time or Permanent",
    "ACTION",
    "If In Stock: Action",
    "Buysmart Action",
    "Request Bucket",
    "Outcome Reporting",
    "Needs Review",
    "Validation Status",
    "Analyst Notes",
]


KNOWN_ACTION_OPTIONS = [
    "",
    "OK",
    "1X",
    "NO",
    "Use Right",
    "Find Alt 1st",
    "Check if use right is APL",
    "Check with CDM",
    "Send to CDM",
    "Invalid Information",
    "Produce MOG",
    "Supply America",
    "Buy Direct - United Restaurant Supplies and Equipment",
    "HMSHost",
    "HMS Host",
    "In Stock - Add as PRF",
    "Check for S1 alt, if there isn't one, please approve",
    "On MOG. Check Attribute.",
    "Cannot Add. Not in Stock.",
]


KNOWN_IF_STOCK_OPTIONS = ["", "OK", "NO", "HMSHost", "HMS Host"]
KNOWN_BUYSMART_OPTIONS = ["", "Approved", "Denied", "Assigned"]
KNOWN_STATUS_OPTIONS = ["Ready", "Needs Review", "Excluded"]


WORKFLOW_SEARCH_COLUMNS = [
    "Business",
    "Type",
    "Case#",
    "Division",
    "Sector",
    "Vendor",
    "Unit Name",
    "Description",
    "Manufacturer",
    "Brand",
    "Parent Category",
    "Sub Category",
    "DIN",
    "MIN",
    "ACTION",
    "If In Stock: Action",
    "Buysmart Action",
    "Rule Applied",
    "Validation Status",
    "Analyst Notes",
    "Assignment",
    "Status",
]


OUTCOME_SEARCH_COLUMNS = [
    "Business",
    "Type",
    "Case#",
    "Vendor",
    "Description",
    "ACTION",
    "If In Stock: Action",
    "Buysmart Action",
    "Outcome Reporting",
    "Analyst Notes",
    "Validation Status",
]


def get_session():
    if get_active_session is None:
        return None
    try:
        return get_active_session()
    except Exception:
        return None


def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def lower_text(value: object) -> str:
    return clean_text(value).lower()


def collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not df.columns.duplicated().any():
        return df

    collapsed: dict[str, pd.Series] = {}
    ordered_names = list(dict.fromkeys(df.columns.tolist()))
    for name in ordered_names:
        matches = df.loc[:, df.columns == name]
        if matches.shape[1] == 1:
            collapsed[name] = matches.iloc[:, 0]
            continue
        combined = matches.iloc[:, 0]
        for idx in range(1, matches.shape[1]):
            combined = combined.combine_first(matches.iloc[:, idx])
        collapsed[name] = combined
    return pd.DataFrame(collapsed)


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return collapse_duplicate_columns(out)


def normalize_columns(df: pd.DataFrame, synonyms: Mapping[str, Iterable[str]]) -> pd.DataFrame:
    rename_map: dict[str, str] = {}
    lookup: dict[str, str] = {}
    for canonical, aliases in synonyms.items():
        lookup[canonical.lower()] = canonical
        for alias in aliases:
            lookup[str(alias).strip().lower()] = canonical
    for col in df.columns:
        key = str(col).strip().lower()
        if key in lookup:
            rename_map[col] = lookup[key]
    return df.rename(columns=rename_map)


def ensure_columns(df: pd.DataFrame, required: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    deduped_required = list(dict.fromkeys(required))
    for col in deduped_required:
        if col not in out.columns:
            out[col] = pd.NA
    ordered = deduped_required + [c for c in out.columns if c not in deduped_required]
    return out[ordered]


def normalize_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def normalize_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def normalize_meets_criteria(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    numeric.loc[numeric > 1] = numeric.loc[numeric > 1] / 100.0
    return numeric


def contains_word(series: pd.Series, pattern: str) -> pd.Series:
    return series.fillna("").astype(str).str.contains(pattern, case=False, regex=True, na=False)


def normalize_business_key(value: object) -> str:
    text = lower_text(value).replace(" ", "")
    if "foodbuyone" in text:
        return "FOODBUY_ONE"
    if "hmshost" in text:
        return "HMSHOST"
    if "healthtrust" in text:
        return "HEALTHTRUST"
    if "canada" in text:
        return "CANADA"
    if "compass" in text:
        return "COMPASS"
    return "OTHER"


def normalize_request_type_key(value: object) -> str:
    text = lower_text(value)
    if "mass add" in text and "srf" in text:
        return "MASS_SRF"
    if "mass add" in text or text == "mass adds":
        return "MASS_ADD"
    if text == "prf":
        return "PRF"
    if text == "sorf":
        return "SORF"
    if text == "srf":
        return "SRF"
    return text.upper()


def normalize_in_cat_key(value: object) -> str:
    text = lower_text(value)
    if text in {"y", "yes"}:
        return "Y"
    if text in {"n", "no"}:
        return "N"
    if text in {"ta", "temp available", "temporary available"}:
        return "TA"
    if text == "a":
        return "A"
    return text.upper()


def canonical_action_key(value: object) -> str:
    text = lower_text(value)
    if not text:
        return ""
    if "deleted, no spend" in text:
        return "DELETED_NO_SPEND"
    if "cannot add" in text:
        return "CANNOT_ADD_NOT_IN_STOCK"
    if "on mog" in text:
        return "ON_MOG_CHECK_ATTRIBUTE"
    if text in {"ok", "ok.", "approve", "approved"}:
        return "OK"
    if text in {"1x", "one-time", "one time"}:
        return "1X"
    if text in {"no", "denied", "deny"}:
        return "NO"
    if "use right" in text:
        return "USE_RIGHT"
    if "find alt" in text:
        return "FIND_ALT_1ST"
    if "check if use right is apl" in text:
        return "CHECK_IF_USE_RIGHT_IS_APL"
    if "check with cdm" in text:
        return "CHECK_WITH_CDM"
    if "send to cdm" in text:
        return "SEND_TO_CDM"
    if "invalid information" in text:
        return "INVALID_INFORMATION"
    if "produce mog" in text:
        return "PRODUCE_MOG"
    if "supply america" in text:
        return "SUPPLY_AMERICA"
    if "buy direct" in text:
        return "BUY_DIRECT"
    if "hmshost" in text or "hms host" in text:
        return "HMSHOST"
    if "in stock" in text and "prf" in text:
        return "IN_STOCK_ADD_AS_PRF"
    if "check for s1 alt" in text:
        return "CHECK_FOR_S1_ALT"
    return text.upper()


def display_action_from_key(key: str) -> str:
    mapping = {
        "OK": "OK",
        "1X": "1X",
        "NO": "NO",
        "USE_RIGHT": "Use Right",
        "FIND_ALT_1ST": "Find Alt 1st",
        "CHECK_IF_USE_RIGHT_IS_APL": "Check if use right is APL",
        "CHECK_WITH_CDM": "Check with CDM",
        "SEND_TO_CDM": "Send to CDM",
        "INVALID_INFORMATION": "Invalid Information",
        "PRODUCE_MOG": "Produce MOG",
        "SUPPLY_AMERICA": "Supply America",
        "BUY_DIRECT": "Buy Direct - United Restaurant Supplies and Equipment",
        "HMSHOST": "HMSHost",
        "IN_STOCK_ADD_AS_PRF": "In Stock - Add as PRF",
        "CHECK_FOR_S1_ALT": "Check for S1 alt, if there isn't one, please approve",
        "ON_MOG_CHECK_ATTRIBUTE": "On MOG. Check Attribute.",
        "CANNOT_ADD_NOT_IN_STOCK": "Cannot Add. Not in Stock.",
    }
    return mapping.get(key, key)


def display_if_stock_from_key(key: str) -> str:
    mapping = {"OK": "OK", "NO": "NO", "HMSHOST": "HMS Host"}
    return mapping.get(key, key)


def normalize_buysmart_key(value: object) -> str:
    text = lower_text(value)
    if text == "approved":
        return "APPROVED"
    if text == "denied":
        return "DENIED"
    if text == "assigned":
        return "ASSIGNED"
    return ""


def display_buysmart_from_key(key: str) -> str:
    return {"APPROVED": "Approved", "DENIED": "Denied", "ASSIGNED": "Assigned"}.get(key, key)


def _quote_ident(identifier: str) -> str:
    value = str(identifier).strip()
    if value.startswith('"') and value.endswith('"'):
        return value
    return '"' + value.replace('"', '""') + '"'


def _split_object_name(object_name: str) -> list[str]:
    return [part.strip().strip('"') for part in str(object_name).split(".") if part.strip()]


def _build_fqn(*parts: str) -> str:
    return ".".join(_quote_ident(part) for part in parts if part)


def get_session_context(session) -> dict[str, str]:
    if session is None:
        return {}
    try:
        df = session.sql(
            """
            select
                current_database() as CURRENT_DATABASE,
                current_schema() as CURRENT_SCHEMA,
                current_role() as CURRENT_ROLE,
                current_warehouse() as CURRENT_WAREHOUSE
            """
        ).to_pandas()
    except Exception:
        return {}
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    return {str(k): ("" if pd.isna(v) else str(v)) for k, v in row.items()}


def _describe_table(session, table_name: str) -> pd.DataFrame:
    if session is None:
        return pd.DataFrame()
    try:
        return session.sql(f"desc table {table_name}").to_pandas()
    except Exception:
        return pd.DataFrame()


def _extract_desc_column_names(desc_df: pd.DataFrame) -> list[str]:
    if desc_df.empty:
        return []
    name_col = next((col for col in desc_df.columns if str(col).lower() == "name"), None)
    if name_col is None:
        return []
    return [str(name).upper() for name in desc_df[name_col].tolist()]


def resolve_table_name(session, table_name: str) -> str | None:
    cache = st.session_state.setdefault("_resolved_table_names", {})
    key = str(table_name).strip()
    if key in cache:
        return cache[key]

    context = get_session_context(session)
    current_db = context.get("CURRENT_DATABASE", "")
    current_schema = context.get("CURRENT_SCHEMA", "")
    parts = _split_object_name(key)
    candidates: list[str] = []

    if not parts:
        cache[key] = None
        return None
    if _extract_desc_column_names(_describe_table(session, key)):
        cache[key] = key
        return key

    if len(parts) == 3:
        candidates.append(_build_fqn(parts[0], parts[1], parts[2]))
    elif len(parts) == 2 and current_db:
        candidates.append(_build_fqn(current_db, parts[0], parts[1]))
    else:
        object_name = parts[0]
        if TABLE_DATABASE and TABLE_SCHEMA:
            candidates.append(_build_fqn(TABLE_DATABASE, TABLE_SCHEMA, object_name))
        if current_db and current_schema:
            candidates.append(_build_fqn(current_db, current_schema, object_name))

    for candidate in list(dict.fromkeys(candidates)):
        if _extract_desc_column_names(_describe_table(session, candidate)):
            cache[key] = candidate
            return candidate

    cache[key] = None
    return None


def get_table_columns(session, table_name: str) -> list[str]:
    resolved_name = resolve_table_name(session, table_name)
    if not resolved_name:
        return []
    return _extract_desc_column_names(_describe_table(session, resolved_name))


def load_table_if_exists(session, table_name: str) -> pd.DataFrame:
    resolved_name = resolve_table_name(session, table_name)
    if not resolved_name:
        return pd.DataFrame()
    try:
        return session.sql(f"select * from {resolved_name}").to_pandas()
    except Exception:
        return pd.DataFrame()


def build_missing_table_message(session, table_name: str) -> str:
    context = get_session_context(session)
    pieces = [f"Could not resolve table {table_name}."]
    if context:
        pieces.append(
            "Session context: "
            f"database={context.get('CURRENT_DATABASE') or '[none]'}, "
            f"schema={context.get('CURRENT_SCHEMA') or '[none]'}, "
            f"role={context.get('CURRENT_ROLE') or '[none]'}."
        )
    pieces.append("Use a fully qualified table name or set TABLE_DATABASE / TABLE_SCHEMA at the top of the app.")
    return " ".join(pieces)


def load_workflow_sheet(uploaded_file) -> tuple[pd.DataFrame, str]:
    sheet_name = "Sheet1"
    try:
        excel_file = pd.ExcelFile(uploaded_file)
        sheet_name = excel_file.sheet_names[0]
        workflow = excel_file.parse(sheet_name=sheet_name)
    except Exception as exc:
        st.error(f"Unable to read workbook: {exc}")
        return pd.DataFrame(), sheet_name

    workflow = sanitize_dataframe(workflow)
    workflow = normalize_columns(workflow, COLUMN_ALIASES)
    workflow = collapse_duplicate_columns(workflow)
    workflow = ensure_columns(workflow, SOURCE_COLUMNS)
    return collapse_duplicate_columns(workflow), sheet_name


def append_text_field(df: pd.DataFrame, mask: pd.Series, column: str, value: str) -> None:
    if not mask.any():
        return
    existing = df.loc[mask, column].fillna("").astype(str).str.strip()
    updated = existing.where(existing == "", existing + "; " + value)
    updated = updated.where(existing != "", value)
    df.loc[mask, column] = updated


def append_rule(df: pd.DataFrame, mask: pd.Series, rule_id: str) -> None:
    append_text_field(df, mask, "Rule Applied", rule_id)


def append_note(df: pd.DataFrame, mask: pd.Series, note: str) -> None:
    append_text_field(df, mask, "Analyst Notes", note)
    if mask.any():
        df.loc[mask, "Needs Review"] = True


def set_action_key(df: pd.DataFrame, mask: pd.Series, action_key: str, rule_id: str | None = None) -> None:
    if not mask.any():
        return
    df.loc[mask, "ACTION"] = display_action_from_key(action_key)
    if rule_id:
        append_rule(df, mask, rule_id)


def set_if_stock_key(df: pd.DataFrame, mask: pd.Series, action_key: str, rule_id: str | None = None) -> None:
    if not mask.any():
        return
    df.loc[mask, "If In Stock: Action"] = display_if_stock_from_key(action_key)
    if rule_id:
        append_rule(df, mask, rule_id)


def set_buysmart_key(df: pd.DataFrame, mask: pd.Series, buy_key: str, rule_id: str | None = None) -> None:
    if not mask.any():
        return
    df.loc[mask, "Buysmart Action"] = display_buysmart_from_key(buy_key)
    if rule_id:
        append_rule(df, mask, rule_id)


def approve_with_stock_context(df: pd.DataFrame, mask: pd.Series, rule_id: str) -> None:
    if not mask.any():
        return

    stock_context = df["upstream_action_key"].isin({"ON_MOG_CHECK_ATTRIBUTE", "CANNOT_ADD_NOT_IN_STOCK"})
    preserve_mask = mask & stock_context
    if preserve_mask.any():
        df.loc[preserve_mask, "ACTION"] = df.loc[preserve_mask, "Upstream Action"]
        df.loc[preserve_mask, "If In Stock: Action"] = "OK"
        append_rule(df, preserve_mask, rule_id)

    non_stock = mask & ~stock_context
    if (non_stock & df["is_one_time"]).any():
        set_action_key(df, non_stock & df["is_one_time"], "1X", rule_id)
    if (non_stock & ~df["is_one_time"]).any():
        set_action_key(df, non_stock & ~df["is_one_time"], "OK", rule_id)


def deny_with_stock_context(df: pd.DataFrame, mask: pd.Series, rule_id: str) -> None:
    if not mask.any():
        return
    cannot_add = mask & (df["upstream_action_key"] == "CANNOT_ADD_NOT_IN_STOCK")
    on_mog = mask & (df["upstream_action_key"] == "ON_MOG_CHECK_ATTRIBUTE")
    other = mask & ~(cannot_add | on_mog)
    if cannot_add.any():
        df.loc[cannot_add, "ACTION"] = df.loc[cannot_add, "Upstream Action"]
        df.loc[cannot_add, "If In Stock: Action"] = "NO"
        append_rule(df, cannot_add, rule_id)
    if on_mog.any():
        set_action_key(df, on_mog, "NO", rule_id)
    if other.any():
        set_action_key(df, other, "NO", rule_id)


def prepare_upstream_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Upstream Action" not in out.columns or out["Upstream Action"].fillna("").astype(str).str.strip().eq("").all():
        out["Upstream Action"] = out["ACTION"]
    if "Upstream If In Stock: Action" not in out.columns or out["Upstream If In Stock: Action"].fillna("").astype(str).str.strip().eq("").all():
        out["Upstream If In Stock: Action"] = out["If In Stock: Action"]
    if "Upstream Buysmart Action" not in out.columns or out["Upstream Buysmart Action"].fillna("").astype(str).str.strip().eq("").all():
        out["Upstream Buysmart Action"] = out["Buysmart Action"]
    return out


def build_request_id(row: pd.Series, reporting_date: date, source_file_name: str = "", source_sheet_name: str = "") -> str:
    existing = clean_text(row.get("workflow_request_id"))
    if existing:
        return existing
    signature_parts = [
        str(reporting_date),
        source_file_name,
        source_sheet_name,
        clean_text(row.get("source_row_number")),
        clean_text(row.get("Case#")),
        clean_text(row.get("Business")),
        clean_text(row.get("Type")),
        clean_text(row.get("Vendor")),
        clean_text(row.get("DIN")),
        clean_text(row.get("MIN")),
        clean_text(row.get("Description")),
        clean_text(row.get("Unit Number")),
        clean_text(row.get("Date Created")),
    ]
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, "|".join(signature_parts)))


def build_sample_workflow(reporting_date: date) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Business": ["Compass USA", "Compass Canada", "HealthTrust", "HMSHost"],
            "Type": ["PRF", "Mass Adds", "SRF", "PRF"],
            "Case#": ["WO0000001", pd.NA, "WO0000003", "WO0000004"],
            "Date Created": pd.to_datetime([reporting_date] * 4),
            "Sector": ["Morrison", "Chartwells Canada", "Acute Care", "Airport"],
            "Division": ["Healthcare Division", "Schools Division", "HealthTrust", "HMSHost"],
            "Unit Name": ["Unit A", "Unit B", "Unit C", "Unit D"],
            "Unit Number": ["1001", "1002", "1003", "1004"],
            "Vendor": ["Sysco Houston", "Sysco Vancouver", "US Foods Port Orange 3055 5Z", "Sysco Metro NY - Ritter"],
            "Parent Category": ["Protein", "Disposables - Containers and Dinnerware", "Bakery & Dessert", "Beverages"],
            "Sub Category": ["Chicken Breast Unbreaded Raw", "Smallwares", "Soup Frozen", "Soda"],
            "DIN": ["111111", "222222", "333333", "444444"],
            "MIN": ["MIN1", "MIN2", "MIN3", "MIN4"],
            "Manufacturer": ["Great Lakes", "Diversey", "Chef Francisco", "Coca Cola Bottling Company"],
            "Brand": ["Sysco", "Diversey", "Chef Francisco", "Coke"],
            "Description": ["Chicken Breast Raw", "Disposable tray", "Frozen soup", "Soda fountain syrup"],
            "Usage": [12, 4, 3, 8],
            "One-Time or Permanent": ["Permanent", "Permanent", "One-Time", "Permanent"],
            "Reason for request": ["Expand Program", "", "Patient need", ""],
            "Buysmart Action": [pd.NA, pd.NA, pd.NA, pd.NA],
            "Meets Criteria": [0.0, 0.0, 0.12, 0.0],
            "In CAT": ["Y", "A", "Y", "N"],
            "On MOG": [pd.NA, "SYSCO VANCOUVER - MMM", pd.NA, pd.NA],
            "Pantry": [pd.NA, pd.NA, pd.NA, pd.NA],
            "K12 APL": [pd.NA, "Y", pd.NA, pd.NA],
            "Compass APL": [pd.NA, "Core APL", "S1", pd.NA],
            "ACTION": ["On MOG. Check Attribute.", "On MOG. Check Attribute.", "", "Cannot Add. Not in Stock."],
            "If In Stock: Action": [pd.NA, pd.NA, pd.NA, pd.NA],
            "Conversion DIN": [pd.NA, pd.NA, pd.NA, pd.NA],
            "Audit Action": [pd.NA, pd.NA, pd.NA, pd.NA],
        }
    )


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    text_cols = [
        "Business",
        "Type",
        "Vendor",
        "Brand",
        "Manufacturer",
        "Description",
        "Sub Category",
        "Parent Category",
        "Division",
        "Sector",
        "Reason for request",
        "DIN",
        "MIN",
        "One-Time or Permanent",
        "In CAT",
        "Pantry",
        "K12 APL",
        "Compass APL",
        "Conversion DIN",
        "ACTION",
        "If In Stock: Action",
        "Buysmart Action",
        "Upstream Action",
        "Upstream If In Stock: Action",
    ]
    for col in text_cols:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).str.strip()

    out["Usage"] = normalize_numeric(out["Usage"])
    out["Date Created"] = normalize_date(out["Date Created"])
    out["Meets Criteria"] = normalize_meets_criteria(out["Meets Criteria"])

    out["business_key"] = out["Business"].apply(normalize_business_key)
    out["request_type_key"] = out["Type"].apply(normalize_request_type_key)
    out["is_one_time"] = out["One-Time or Permanent"].str.lower().eq("one-time")
    out["is_permanent"] = out["One-Time or Permanent"].str.lower().eq("permanent")
    out["usage_num"] = out["Usage"].fillna(0)
    out["meets_criteria_num"] = out["Meets Criteria"].fillna(0)
    out["meets_criteria_ge_10"] = out["meets_criteria_num"] >= 0.10
    out["in_cat_key"] = out["In CAT"].apply(normalize_in_cat_key)
    out["is_in_cat_y"] = out["in_cat_key"].eq("Y")
    out["is_in_catalog"] = out["in_cat_key"].isin(["Y", "A"])
    out["is_pantry"] = out["Pantry"].ne("")
    out["is_k12_apl"] = out["K12 APL"].str.upper().eq("Y")
    apl = out["Compass APL"].fillna("").astype(str)
    out["is_core_apl"] = apl.str.contains("core apl", case=False, na=False)
    out["is_s1"] = apl.str.contains("s1", case=False, na=False)
    out["is_foh"] = apl.str.contains("foh", case=False, na=False)
    out["is_diverse"] = apl.str.contains("diverse", case=False, na=False)
    out["has_conversion"] = out["Conversion DIN"].ne("")
    out["upstream_action_key"] = out["Upstream Action"].apply(canonical_action_key)
    out["upstream_if_stock_key"] = out["Upstream If In Stock: Action"].apply(canonical_action_key)
    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    out["current_buysmart_key"] = out["Buysmart Action"].apply(normalize_buysmart_key)

    out["brand_lc"] = out["Brand"].str.lower()
    out["manufacturer_lc"] = out["Manufacturer"].str.lower()
    out["description_lc"] = out["Description"].str.lower()
    out["subcategory_lc"] = out["Sub Category"].str.lower()
    out["parent_category_lc"] = out["Parent Category"].str.lower()
    out["division_lc"] = out["Division"].str.lower()
    out["sector_lc"] = out["Sector"].str.lower()
    out["reason_lc"] = out["Reason for request"].str.lower()
    out["vendor_lc"] = out["Vendor"].str.lower()
    out["din_lc"] = out["DIN"].str.lower()
    out["min_lc"] = out["MIN"].str.lower()

    out["is_levy"] = out["division_lc"].str.contains("levy", na=False) | out["sector_lc"].str.contains("levy", na=False)
    out["is_foodbuyone"] = out["business_key"].eq("FOODBUY_ONE")
    out["is_hmshost"] = out["business_key"].eq("HMSHOST")
    out["is_canada"] = out["business_key"].eq("CANADA")
    out["is_healthtrust"] = out["business_key"].eq("HEALTHTRUST")
    out["is_compass"] = out["business_key"].eq("COMPASS")
    out["is_mass_add"] = out["request_type_key"].eq("MASS_ADD")
    out["is_mass_srf"] = out["request_type_key"].eq("MASS_SRF")
    out["is_prf"] = out["request_type_key"].eq("PRF")
    out["is_sorf"] = out["request_type_key"].eq("SORF")
    out["is_srf"] = out["request_type_key"].eq("SRF")
    return out


def load_reference_data(session) -> dict[str, object]:
    refs: dict[str, object] = {}

    local_vendor_df = load_table_if_exists(session, TABLE_LOCAL_VENDOR)
    if not local_vendor_df.empty and "VENDOR_NAME" in local_vendor_df.columns:
        refs["local_vendors"] = set(local_vendor_df["VENDOR_NAME"].dropna().astype(str).str.strip().str.lower())
    else:
        refs["local_vendors"] = {v.lower() for v in DEFAULT_LOCAL_VENDORS}

    disallowed_min_df = load_table_if_exists(session, TABLE_DISALLOWED_MIN)
    if not disallowed_min_df.empty and "MIN" in disallowed_min_df.columns:
        refs["disallowed_mins"] = set(disallowed_min_df["MIN"].dropna().astype(str).str.strip().str.lower())
    else:
        refs["disallowed_mins"] = {m.lower() for m, _ in DEFAULT_DISALLOWED_HIGLINER}

    allowlist_df = load_table_if_exists(session, TABLE_ALLOWLIST)
    if not allowlist_df.empty and "MIN" in allowlist_df.columns:
        refs["lamb_weston_allowlist"] = set(allowlist_df["MIN"].dropna().astype(str).str.strip().str.lower())
    else:
        refs["lamb_weston_allowlist"] = {row["MIN"].lower() for row in DEFAULT_LAMB_WESTON_ALLOWLIST}

    refs["rule_catalog"] = load_table_if_exists(session, TABLE_RULE_CATALOG)
    return refs


def prepare_workflow_dataframe(
    session,
    workflow_source: pd.DataFrame | None,
    reporting_date: date,
    *,
    batch_id: str | None = None,
    source_file_name: str | None = None,
    source_sheet_name: str | None = None,
) -> pd.DataFrame:
    if workflow_source is None or workflow_source.empty:
        workflow = build_sample_workflow(reporting_date)
    else:
        workflow = workflow_source.copy()

    workflow = sanitize_dataframe(workflow)
    workflow = normalize_columns(workflow, COLUMN_ALIASES)
    workflow = collapse_duplicate_columns(workflow)
    workflow = ensure_columns(workflow, SOURCE_COLUMNS + APP_COLUMNS)
    workflow = collapse_duplicate_columns(workflow)
    workflow = prepare_upstream_fields(workflow)

    workflow["Selected"] = coerce_bool_series(workflow["Selected"], default=False)
    workflow["Excluded"] = coerce_bool_series(workflow["Excluded"], default=False)
    workflow["Needs Review"] = coerce_bool_series(workflow["Needs Review"], default=False)
    workflow["is_active"] = coerce_bool_series(workflow["is_active"], default=True)

    if batch_id is not None:
        workflow["batch_id"] = batch_id
    if source_file_name is not None:
        workflow["source_file_name"] = source_file_name
    if source_sheet_name is not None:
        workflow["source_sheet_name"] = source_sheet_name

    if workflow["source_row_number"].fillna("").astype(str).str.strip().eq("").all():
        workflow["source_row_number"] = range(1, len(workflow) + 1)

    workflow["reporting_date"] = pd.to_datetime(reporting_date).date()
    workflow["Last Sync"] = pd.to_datetime(workflow["Last Sync"], errors="coerce")
    workflow["Last Saved"] = pd.to_datetime(workflow["Last Saved"], errors="coerce")

    workflow["workflow_request_id"] = [
        build_request_id(
            row,
            reporting_date,
            clean_text(source_file_name or row.get("source_file_name")),
            clean_text(source_sheet_name or row.get("source_sheet_name")),
        )
        for _, row in workflow.iterrows()
    ]

    return add_derived_columns(workflow)


def start_rule_run(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ACTION"] = out["Upstream Action"].fillna("").astype(str).str.strip()
    out["If In Stock: Action"] = out["Upstream If In Stock: Action"].fillna("").astype(str).str.strip()
    out["Buysmart Action"] = ""
    out["Rule Applied"] = ""
    out["Validation Status"] = ""
    out["Excluded"] = False
    out["Excluded Reason"] = ""
    audit_notes = out["Audit Action"].fillna("").astype(str).str.strip()
    existing_notes = out["Analyst Notes"].fillna("").astype(str).str.strip()
    out["Analyst Notes"] = audit_notes.where(audit_notes.ne(""), existing_notes)
    out["Needs Review"] = False
    out["Queue Bucket"] = ""
    out["Request Bucket"] = ""
    out["Outcome Reporting"] = ""
    return out


def apply_preprocessing_rules(df: pd.DataFrame, refs: dict[str, object]) -> pd.DataFrame:
    out = df.copy()
    local_vendor_mask = out["vendor_lc"].isin(refs["local_vendors"])
    if local_vendor_mask.any():
        out.loc[local_vendor_mask, "Excluded"] = True
        out.loc[local_vendor_mask, "Excluded Reason"] = "Local DC vendor"
        out.loc[local_vendor_mask, "Status"] = "Excluded"
        append_rule(out, local_vendor_mask, "R-001")

    compass_deleted_no_spend = out["is_compass"] & out["upstream_action_key"].eq("DELETED_NO_SPEND")
    if compass_deleted_no_spend.any():
        out.loc[compass_deleted_no_spend, "ACTION"] = ""
        out.loc[compass_deleted_no_spend, "If In Stock: Action"] = ""
        append_rule(out, compass_deleted_no_spend, "R-002")

    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def apply_foodbuy_one_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scope = out["is_foodbuyone"] & ~out["Excluded"]

    one_time_mask = scope & out["is_one_time"]
    if one_time_mask.any():
        set_action_key(out, one_time_mask, "1X", "R-012")
        append_note(out, one_time_mask, "Foodbuy One 1X approved; notify Imogen.")

    approve_mask = scope & ~one_time_mask & (
        out["meets_criteria_ge_10"] | out["is_s1"] | out["is_diverse"] | out["is_foh"]
    )
    if approve_mask.any():
        approve_with_stock_context(out, approve_mask, "R-010")

    fallback_mask = scope & out["ACTION"].fillna("").eq("")
    if fallback_mask.any():
        set_action_key(out, fallback_mask, "FIND_ALT_1ST", "R-011")

    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def apply_hmshost_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scope = out["is_hmshost"] & ~out["Excluded"]
    if scope.any():
        set_action_key(out, scope, "HMSHOST", "R-020")
    stock_mask = scope & out["upstream_action_key"].eq("CANNOT_ADD_NOT_IN_STOCK")
    if stock_mask.any():
        set_if_stock_key(out, stock_mask, "HMSHOST", "R-021")
    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def apply_canada_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scope = out["is_canada"] & ~out["Excluded"]

    diversey_mask = scope & (out["brand_lc"].eq("diversey") | out["manufacturer_lc"].eq("diversey"))
    if diversey_mask.any():
        approve_with_stock_context(out, diversey_mask, "R-039")

    smallwares_mask = scope & contains_word(out["Parent Category"], r"smallwares")
    if smallwares_mask.any():
        set_action_key(out, smallwares_mask, "BUY_DIRECT", "R-038")

    has_conversion = scope & out["has_conversion"]
    if has_conversion.any():
        set_action_key(out, has_conversion, "CHECK_IF_USE_RIGHT_IS_APL", "R-031")
        append_note(out, has_conversion & out["is_one_time"] & (out["usage_num"] > 10), "Escalate Canada 1X > 10 cases.")

    approval_mask = scope & ~has_conversion & (out["is_core_apl"] | out["is_s1"] | out["is_pantry"])
    if approval_mask.any():
        approve_with_stock_context(out, approval_mask, "R-030")

    low_usage_one_time = scope & ~has_conversion & out["is_one_time"] & ~(
        out["is_core_apl"] | out["is_s1"] | out["is_pantry"]
    ) & (out["usage_num"] <= 10)
    if low_usage_one_time.any():
        set_action_key(out, low_usage_one_time, "1X", "R-033")

    deny_mask = scope & out["ACTION"].fillna("").eq("") & ~(
        diversey_mask | smallwares_mask | has_conversion | approval_mask | low_usage_one_time
    )
    if deny_mask.any():
        deny_with_stock_context(out, deny_mask, "R-037")

    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def apply_healthtrust_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scope = out["is_healthtrust"] & ~out["Excluded"]

    invalid_info = scope & (out["MIN"].eq("") | out["DIN"].eq(""))
    if invalid_info.any():
        set_action_key(out, invalid_info, "INVALID_INFORMATION", "R-060")

    approved_brand = out["brand_lc"].isin(APPROVED_BRANDS_HEALTHTRUST)
    srf_scope = scope & out["is_srf"]
    srf_approve = srf_scope & (out["is_s1"] | approved_brand)
    if srf_approve.any():
        set_action_key(out, srf_approve, "OK", "R-050")

    srf_guided = srf_scope & ~srf_approve & out["meets_criteria_ge_10"]
    if srf_guided.any():
        set_action_key(out, srf_guided, "CHECK_FOR_S1_ALT", "R-051")
        append_note(out, srf_guided, "Confirm there is no S1 or higher-VA alternative before approval.")

    srf_no = srf_scope & out["ACTION"].fillna("").eq("")
    if srf_no.any():
        set_action_key(out, srf_no, "NO", "R-052")

    prf_sorf_scope = scope & (out["is_prf"] | out["is_sorf"])
    conversion_mask = prf_sorf_scope & out["has_conversion"]
    if conversion_mask.any():
        set_action_key(out, conversion_mask, "USE_RIGHT", "R-053")

    raw_chicken_mask = scope & contains_word(out["Sub Category"], r"chicken breast unbreaded raw")
    if raw_chicken_mask.any():
        set_action_key(out, raw_chicken_mask, "SEND_TO_CDM", "R-055")
        append_note(out, raw_chicken_mask, "4 oz / 5 oz cases need supplier matrix review.")

    approve_standard = scope & ~raw_chicken_mask & ~conversion_mask & (
        out["is_s1"] | out["is_diverse"] | out["is_foh"] | approved_brand | out["meets_criteria_ge_10"]
    )
    if approve_standard.any():
        approve_with_stock_context(out, approve_standard, "R-054")

    unresolved = scope & out["ACTION"].fillna("").eq("")
    if unresolved.any():
        append_note(out, unresolved, "HealthTrust row needs analyst judgment after deterministic rules.")
        append_rule(out, unresolved, "R-059")

    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def apply_compass_srf_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scope = out["is_compass"] & out["is_srf"] & ~out["Excluded"]

    k12_conversion_override = scope & out["has_conversion"] & out["is_k12_apl"] & contains_word(out["Division"], r"schools")
    if k12_conversion_override.any():
        out.loc[k12_conversion_override, "Conversion DIN"] = ""
        append_rule(out, k12_conversion_override, "R-071")
        append_note(out, k12_conversion_override, "K12 conversion override applied; requested item used instead of conversion.")

    use_right_mask = scope & out["has_conversion"] & ~k12_conversion_override
    if use_right_mask.any():
        set_action_key(out, use_right_mask, "USE_RIGHT", "R-070")

    approve_brand = out["brand_lc"].isin(APPROVED_BRANDS_COMPASS)
    approve_scope = scope & (out["is_s1"] | out["is_diverse"] | out["is_foh"] | approve_brand)
    if approve_scope.any():
        set_action_key(out, approve_scope, "OK", "R-072")

    k12_scope = scope & contains_word(out["Division"], r"schools|chartwells") & out["is_k12_apl"]
    if k12_scope.any():
        set_action_key(out, k12_scope, "OK", "R-073")

    alt_check = scope & out["ACTION"].fillna("").eq("") & out["meets_criteria_ge_10"]
    if alt_check.any():
        set_action_key(out, alt_check, "CHECK_FOR_S1_ALT", "R-075")
        append_note(out, alt_check, "Compass SRF: confirm no S1 / higher-VA alternative.")

    on_mog_stock = scope & out["upstream_action_key"].eq("ON_MOG_CHECK_ATTRIBUTE") & out["is_in_catalog"]
    if on_mog_stock.any():
        out.loc[on_mog_stock, "ACTION"] = out.loc[on_mog_stock, "Upstream Action"]
        out.loc[on_mog_stock, "If In Stock: Action"] = "OK"
        append_rule(out, on_mog_stock, "R-076")

    in_catalog_not_marked = scope & out["ACTION"].fillna("").eq("") & out["is_in_catalog"]
    if in_catalog_not_marked.any():
        set_action_key(out, in_catalog_not_marked, "IN_STOCK_ADD_AS_PRF", "R-077")

    no_mask = scope & out["ACTION"].fillna("").eq("")
    if no_mask.any():
        set_action_key(out, no_mask, "NO", "R-078")

    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def apply_compass_prf_sorf_rules(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    scope = out["is_compass"] & (out["is_prf"] | out["is_sorf"]) & ~out["Excluded"]

    conversion_mask = scope & out["has_conversion"]
    low_usage_conversion = conversion_mask & out["is_one_time"] & (out["usage_num"] < 15) & out["ACTION"].fillna("").eq("")
    if low_usage_conversion.any():
        set_action_key(out, low_usage_conversion, "1X", "R-079")
        append_note(out, low_usage_conversion, "Low-usage 1X conversion approved; confirm before removing conversion.")

    sponsored_conversion = conversion_mask & contains_word(out["Reason for request"], r"sponsorship|allocation|commodity")
    if sponsored_conversion.any():
        approve_with_stock_context(out, sponsored_conversion, "R-079")

    remaining_conversion = conversion_mask & out["ACTION"].fillna("").eq("")
    if remaining_conversion.any():
        set_action_key(out, remaining_conversion, "USE_RIGHT", "R-070")

    approve_brand = out["brand_lc"].isin(APPROVED_BRANDS_COMPASS)
    approve_scope = scope & ~conversion_mask & (
        out["is_s1"] | out["is_diverse"] | out["is_foh"] | approve_brand
    )
    if approve_scope.any():
        approve_with_stock_context(out, approve_scope, "R-080")

    k12_scope = scope & ~conversion_mask & contains_word(out["Division"], r"schools|chartwells") & out["is_k12_apl"]
    if k12_scope.any():
        approve_with_stock_context(out, k12_scope, "R-082")

    pantry_scope = scope & ~conversion_mask & out["is_pantry"]
    if pantry_scope.any():
        approve_with_stock_context(out, pantry_scope, "R-083")

    criteria_scope = scope & ~conversion_mask & out["meets_criteria_ge_10"]
    if criteria_scope.any():
        approve_with_stock_context(out, criteria_scope, "R-084")

    sponsorship_reason = scope & ~conversion_mask & contains_word(out["Reason for request"], r"sponsorship")
    if sponsorship_reason.any():
        approve_with_stock_context(out, sponsorship_reason, "R-079")

    halal_scope = scope & contains_word(out["Description"], r"halal")
    if halal_scope.any():
        approve_with_stock_context(out, halal_scope, "R-085")

    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def apply_compass_exception_overrides(df: pd.DataFrame, refs: dict[str, object]) -> pd.DataFrame:
    out = df.copy()
    scope = out["is_compass"] & ~out["Excluded"]

    french_fries_no = scope & contains_word(out["Sub Category"], r"french fries") & ~out["brand_lc"].eq("mccain")
    if french_fries_no.any():
        set_action_key(out, french_fries_no, "NO", "R-086")

    molded_fiber_no = scope & contains_word(out["Sub Category"], r"molded fiber")
    if molded_fiber_no.any():
        set_action_key(out, molded_fiber_no, "NO", "R-087")
        append_note(out, molded_fiber_no, "Huhtamaki is preferred.")

    smallwares_scope = scope & contains_word(out["Parent Category"], r"smallwares")
    if smallwares_scope.any():
        set_action_key(out, smallwares_scope, "SUPPLY_AMERICA", "R-088")

    frozen_soup_no = scope & contains_word(out["Sub Category"], r"soup frozen") & ~out["brand_lc"].eq("chef francisco")
    if frozen_soup_no.any():
        set_action_key(out, frozen_soup_no, "NO", "R-089")

    produce_mog = scope & contains_word(out["Sub Category"], r"fresh") & (
        contains_word(out["Description"], r"vegetable") | contains_word(out["Description"], r"fruit")
    )
    if produce_mog.any():
        set_action_key(out, produce_mog, "PRODUCE_MOG", "R-091")

    lamb_weston_no = scope & out["brand_lc"].eq("lamb weston") & ~out["min_lc"].isin(refs["lamb_weston_allowlist"])
    if lamb_weston_no.any():
        set_action_key(out, lamb_weston_no, "FIND_ALT_1ST", "R-094")
        append_note(out, lamb_weston_no, "Find McCain Alt unless item is on approved Lamb Weston allowlist.")

    higliner_no = scope & out["min_lc"].isin(refs["disallowed_mins"])
    if higliner_no.any():
        set_action_key(out, higliner_no, "NO", "R-093")

    approved_brand_scope = scope & out["brand_lc"].isin(SPECIAL_COMPASS_APPROVED_BRANDS)
    if approved_brand_scope.any():
        approve_with_stock_context(out, approved_brand_scope, "R-092")

    approved_manufacturer_scope = scope & out["manufacturer_lc"].isin(SPECIAL_COMPASS_APPROVED_MANUFACTURERS)
    if approved_manufacturer_scope.any():
        approve_with_stock_context(out, approved_manufacturer_scope, "R-092")

    soda_stream_prf = scope & out["brand_lc"].isin(SPECIAL_COMPASS_PRF_ONLY_BRANDS) & out["is_prf"]
    if soda_stream_prf.any():
        approve_with_stock_context(out, soda_stream_prf, "R-092")

    ballard_morrison = scope & out["brand_lc"].isin(MORRISON_BALLARD_SUBBRANDS) & contains_word(
        out["Sector"].fillna("").astype(str) + " " + out["Division"].fillna("").astype(str),
        r"morrison",
    )
    if ballard_morrison.any():
        approve_with_stock_context(out, ballard_morrison, "R-092")

    high_usage_one_time = scope & out["is_one_time"] & (out["usage_num"] >= 15) & (out["meets_criteria_num"] <= 0)
    if high_usage_one_time.any():
        append_note(out, high_usage_one_time, "Compass 1X with usage >= 15 and 0% VA should be reviewed.")
        append_rule(out, high_usage_one_time, "R-095")

    chicken_raw_guided = scope & contains_word(out["Sub Category"], r"chicken breast unbreaded raw")
    if chicken_raw_guided.any():
        append_note(out, chicken_raw_guided, "Chicken Breast Unbreaded Raw has matrix / nutritional / sector exceptions.")
        append_rule(out, chicken_raw_guided, "R-092")

    unresolved = scope & out["ACTION"].fillna("").eq("")
    if unresolved.any():
        append_note(out, unresolved, "Compass row still needs analyst judgment after deterministic Alpha rules.")
        append_rule(out, unresolved, "R-092")

    out["current_action_key"] = out["ACTION"].apply(canonical_action_key)
    return out


def derive_buysmart(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    cl_alt = contains_word(out["Description"], r"\^")
    if cl_alt.any():
        set_buysmart_key(out, cl_alt, "ASSIGNED", "R-110")

    new_din = out["DIN"].fillna("").astype(str).str.upper().str.startswith("NEW")
    if new_din.any():
        set_buysmart_key(out, new_din, "ASSIGNED", "R-112")

    mass_add = out["request_type_key"].isin(["MASS_ADD", "MASS_SRF"])
    if mass_add.any():
        set_buysmart_key(out, mass_add, "ASSIGNED", "R-115")

    approved = (
        out["business_key"].eq("COMPASS")
        & out["is_prf"]
        & out["is_permanent"]
        & out["is_in_cat_y"]
        & out["ACTION"].fillna("").astype(str).str.strip().str.lower().isin(["ok", "on mog. check attribute."])
    )
    if approved.any():
        set_buysmart_key(out, approved, "APPROVED", "R-111")

    denied = (
        out["ACTION"].fillna("").astype(str).str.strip().str.lower().eq("cannot add. not in stock.")
        & out["in_cat_key"].isin(["N", "TA"])
    )
    if denied.any():
        set_buysmart_key(out, denied, "DENIED", "R-113")

    blank_remaining = out["Buysmart Action"].fillna("").astype(str).str.strip().eq("")
    if blank_remaining.any():
        set_buysmart_key(out, blank_remaining, "ASSIGNED", "R-114")

    out["current_buysmart_key"] = out["Buysmart Action"].apply(normalize_buysmart_key)
    return out


def run_validations(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    blank_action = (~out["Excluded"]) & out["ACTION"].fillna("").astype(str).str.strip().eq("")
    if blank_action.any():
        append_text_field(out, blank_action, "Validation Status", "Blank ACTION")
        append_rule(out, blank_action, "R-003")
        out.loc[blank_action, "Needs Review"] = True

    one_time_ok = (~out["Excluded"]) & out["is_one_time"] & out["ACTION"].fillna("").astype(str).str.strip().str.lower().eq("ok")
    if one_time_ok.any():
        append_text_field(out, one_time_ok, "Validation Status", "1X row should not end as OK")
        out.loc[one_time_ok, "Needs Review"] = True

    stock_context_missing = (~out["Excluded"]) & out["ACTION"].fillna("").astype(str).str.strip().str.lower().isin(
        ["on mog. check attribute.", "cannot add. not in stock.", "hmshost"]
    ) & out["If In Stock: Action"].fillna("").astype(str).str.strip().eq("")
    if stock_context_missing.any():
        append_text_field(out, stock_context_missing, "Validation Status", "Missing If In Stock: Action for stock-context row")
        out.loc[stock_context_missing, "Needs Review"] = True

    conversion_bad = (~out["Excluded"]) & out["has_conversion"] & ~out["ACTION"].fillna("").astype(str).str.strip().str.lower().isin(
        ["use right", "check if use right is apl", "1x", "ok"]
    )
    if conversion_bad.any():
        append_text_field(out, conversion_bad, "Validation Status", "Conversion row needs use right / Canada check / analyst review")
        out.loc[conversion_bad, "Needs Review"] = True

    return out


def classify_outcome_reporting(row: pd.Series) -> str:
    buy_smart = lower_text(row.get("Buysmart Action"))
    action = lower_text(row.get("ACTION"))
    needs_review = bool(row.get("Needs Review", False))
    one_time = lower_text(row.get("One-Time or Permanent")) == "one-time"

    if buy_smart == "denied" or action == "no":
        return "denied"
    if buy_smart == "approved" and one_time:
        return "1x approved"
    if buy_smart == "approved":
        return "approved"
    if "use right" in action or "use right" in buy_smart:
        return "use right"
    if "find alt" in action or "find alt" in buy_smart:
        return "find alt first"
    if "cdm" in action or "cdm" in buy_smart:
        return "send/check with CDM"
    if needs_review:
        return "unresolved exceptions"
    return "assigned"


def finalize_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    request_bucket = pd.Series("Special exception / analyst review", index=out.index)
    request_bucket = request_bucket.mask(out["Excluded"], "Excluded / Local DC")
    request_bucket = request_bucket.mask(out["request_type_key"].eq("MASS_ADD"), "Mass Add")
    request_bucket = request_bucket.mask(out["request_type_key"].eq("MASS_SRF"), "Mass SRF")
    request_bucket = request_bucket.mask(out["request_type_key"].eq("PRF"), "PRF")
    request_bucket = request_bucket.mask(out["request_type_key"].eq("SORF"), "SORF")
    request_bucket = request_bucket.mask(out["request_type_key"].eq("SRF"), "SRF")
    request_bucket = request_bucket.mask(out["upstream_action_key"].eq("ON_MOG_CHECK_ATTRIBUTE"), "Already On MOG / Check Attribute")
    request_bucket = request_bucket.mask(out["upstream_action_key"].eq("CANNOT_ADD_NOT_IN_STOCK"), "Cannot Add Not in Stock")
    request_bucket = request_bucket.mask(out["has_conversion"], "Conversion DIN / Use Right")
    request_bucket = request_bucket.mask(out["is_one_time"] & ~out["request_type_key"].isin(["MASS_ADD", "MASS_SRF"]), "1x request")
    request_bucket = request_bucket.mask(out["is_permanent"] & ~out["request_type_key"].isin(["MASS_ADD", "MASS_SRF"]), "Permanent request")
    out["Request Bucket"] = request_bucket

    out["Queue Bucket"] = out["Buysmart Action"].fillna("").replace("", "Assigned")
    out["Outcome Reporting"] = out.apply(classify_outcome_reporting, axis=1)
    out["Last Sync"] = pd.Timestamp(datetime.now())

    status = pd.Series("Ready", index=out.index)
    status = status.mask(out["Excluded"], "Excluded")
    status = status.mask(out["Needs Review"], "Needs Review")
    current_status = out["Status"].fillna("").astype(str).str.strip()
    out["Status"] = current_status.where(current_status.ne(""), status)
    return out


def run_harvested_alpha_rules(df: pd.DataFrame, refs: dict[str, object]) -> pd.DataFrame:
    out = start_rule_run(df)
    out = add_derived_columns(out)
    out = apply_preprocessing_rules(out, refs)
    out = apply_foodbuy_one_rules(out)
    out = apply_hmshost_rules(out)
    out = apply_canada_rules(out)
    out = apply_healthtrust_rules(out)
    out = apply_compass_srf_rules(out)
    out = apply_compass_prf_sorf_rules(out)
    out = apply_compass_exception_overrides(out, refs)
    out = derive_buysmart(out)
    out = run_validations(out)
    out = finalize_derived_fields(out)
    return add_derived_columns(out)


def ui_df_to_db_df(workflow_df: pd.DataFrame) -> pd.DataFrame:
    db_df = workflow_df.copy().rename(columns=UI_TO_DB)
    for ui_col, db_col in UI_TO_DB.items():
        if db_col not in db_df.columns and ui_col in workflow_df.columns:
            db_df[db_col] = workflow_df[ui_col]

    if "DATE_CREATED" in db_df.columns:
        db_df["DATE_CREATED"] = pd.to_datetime(db_df["DATE_CREATED"], errors="coerce").dt.date
    if "REPORTING_DATE" in db_df.columns:
        db_df["REPORTING_DATE"] = pd.to_datetime(db_df["REPORTING_DATE"], errors="coerce").dt.date
    for ts_col in ["LAST_SYNC_AT", "LAST_SAVED_AT"]:
        if ts_col in db_df.columns:
            db_df[ts_col] = pd.to_datetime(db_df[ts_col], errors="coerce")
    for bool_col in ["NEEDS_REVIEW", "EXCLUDED_FLAG", "SELECTED_FLAG", "IS_ACTIVE"]:
        if bool_col in db_df.columns:
            db_df[bool_col] = coerce_bool_series(db_df[bool_col], default=False)
    for numeric_col in ["USAGE_QTY", "MEETS_CRITERIA", "CONVERSION_VA_PCT"]:
        if numeric_col in db_df.columns:
            db_df[numeric_col] = pd.to_numeric(db_df[numeric_col], errors="coerce")
    return db_df


def load_workflow_from_snowflake(session, reporting_date: date) -> pd.DataFrame:
    workflow_table = resolve_table_name(session, TABLE_WORKFLOW)
    if not workflow_table:
        return pd.DataFrame()

    reporting_date_str = pd.to_datetime(reporting_date).strftime("%Y-%m-%d")
    table_cols = get_table_columns(session, TABLE_WORKFLOW)

    where_clauses = []
    if "REPORTING_DATE" in table_cols:
        where_clauses.append(f"REPORTING_DATE = '{reporting_date_str}'")
    if "IS_ACTIVE" in table_cols:
        where_clauses.append("IS_ACTIVE = true")

    sql = f"select * from {workflow_table}"
    if where_clauses:
        sql += " where " + " and ".join(where_clauses)
    if "UPDATED_AT" in table_cols:
        sql += " order by UPDATED_AT desc"
    elif "DATE_CREATED" in table_cols:
        sql += " order by DATE_CREATED desc"

    try:
        df = session.sql(sql).to_pandas()
    except Exception as exc:
        st.error(f"Unable to read from {workflow_table}: {exc}")
        return pd.DataFrame()

    if df.empty:
        return df
    rename_map = {db_col: ui_col for db_col, ui_col in DB_TO_UI.items() if db_col in df.columns}
    df = df.rename(columns=rename_map)
    if "Analyst Notes" in df.columns and "Audit Action" not in df.columns:
        df["Audit Action"] = df["Analyst Notes"]
    return df


def merge_workflow_to_snowflake(session, workflow_df: pd.DataFrame) -> int:
    workflow_table = resolve_table_name(session, TABLE_WORKFLOW)
    if not workflow_table:
        raise RuntimeError(build_missing_table_message(session, TABLE_WORKFLOW))

    db_df = ui_df_to_db_df(workflow_df)
    target_cols = set(get_table_columns(session, TABLE_WORKFLOW))
    if "WORKFLOW_REQUEST_ID" not in target_cols:
        raise RuntimeError(f"{workflow_table} must contain WORKFLOW_REQUEST_ID to support merges.")

    db_df = db_df[[c for c in db_df.columns if c in target_cols]].copy()
    if "WORKFLOW_REQUEST_ID" not in db_df.columns:
        raise RuntimeError("No WORKFLOW_REQUEST_ID column found in prepared dataframe.")

    stage_table = f"TMP_CLAB_PROTO_WORKFLOW_{uuid.uuid4().hex.upper()}"
    session.write_pandas(
        db_df,
        stage_table,
        auto_create_table=True,
        overwrite=True,
        create_temp_table=True,
        quote_identifiers=False,
    )

    merge_cols = [c for c in db_df.columns if c != "WORKFLOW_REQUEST_ID"]
    update_assignments = [f"tgt.{col} = src.{col}" for col in merge_cols]
    if "UPDATED_BY" in target_cols:
        update_assignments.append("tgt.UPDATED_BY = current_user()")
    if "UPDATED_AT" in target_cols:
        update_assignments.append("tgt.UPDATED_AT = current_timestamp()")

    insert_cols = ["WORKFLOW_REQUEST_ID"] + merge_cols
    insert_vals = ["src.WORKFLOW_REQUEST_ID"] + [f"src.{col}" for col in merge_cols]
    if "CREATED_BY" in target_cols:
        insert_cols.append("CREATED_BY")
        insert_vals.append("current_user()")
    if "CREATED_AT" in target_cols:
        insert_cols.append("CREATED_AT")
        insert_vals.append("current_timestamp()")
    if "UPDATED_BY" in target_cols:
        insert_cols.append("UPDATED_BY")
        insert_vals.append("current_user()")
    if "UPDATED_AT" in target_cols:
        insert_cols.append("UPDATED_AT")
        insert_vals.append("current_timestamp()")

    merge_sql = f"""
        merge into {workflow_table} as tgt
        using {stage_table} as src
          on tgt.WORKFLOW_REQUEST_ID = src.WORKFLOW_REQUEST_ID
        when matched then update set
            {", ".join(update_assignments)}
        when not matched then insert (
            {", ".join(insert_cols)}
        ) values (
            {", ".join(insert_vals)}
        )
    """
    session.sql(merge_sql).collect()
    return len(db_df)


def load_rule_catalog_summary(session) -> pd.DataFrame:
    rule_catalog = load_table_if_exists(session, TABLE_RULE_CATALOG)
    if rule_catalog.empty:
        return RULE_SUMMARY_DEFAULT.copy()

    cols = {c.upper(): c for c in rule_catalog.columns}
    if "AUTOMATION_CANDIDATE" not in cols or "ALPHA_RECOMMENDATION" not in cols:
        return RULE_SUMMARY_DEFAULT.copy()

    automation = rule_catalog[cols["AUTOMATION_CANDIDATE"]].fillna("").astype(str).str.strip().str.lower()
    alpha = rule_catalog[cols["ALPHA_RECOMMENDATION"]].fillna("").astype(str).str.strip().str.lower()
    return pd.DataFrame(
        {
            "Metric": RULE_SUMMARY_DEFAULT["Metric"],
            "Count": [
                len(rule_catalog),
                int((automation == "yes").sum()),
                int((automation == "partial").sum()),
                int((automation == "no").sum()),
                int((alpha == "alpha").sum()),
                int((alpha == "guided").sum()),
                int((alpha == "future").sum()),
            ],
        }
    )


def sync_visible_editor_changes(full_df: pd.DataFrame, edited_visible_df: pd.DataFrame) -> pd.DataFrame:
    full_indexed = full_df.set_index("workflow_request_id", drop=False)
    edited_indexed = edited_visible_df.set_index("workflow_request_id", drop=False)

    editable_cols = [
        "ACTION",
        "If In Stock: Action",
        "Buysmart Action",
        "Analyst Notes",
        "Needs Review",
        "Validation Status",
        "Excluded",
        "Excluded Reason",
        "Assignment",
        "Status",
        "Selected",
    ]
    for col in editable_cols:
        if col in edited_indexed.columns and col in full_indexed.columns:
            full_indexed.loc[edited_indexed.index, col] = edited_indexed[col]

    updated = full_indexed.reset_index(drop=True)
    updated["Selected"] = coerce_bool_series(updated["Selected"], default=False)
    updated["Excluded"] = coerce_bool_series(updated["Excluded"], default=False)
    updated["Needs Review"] = coerce_bool_series(updated["Needs Review"], default=False)
    updated = add_derived_columns(updated)
    return finalize_derived_fields(updated)


def initialize_workflow_state_from_table(session, reporting_date: date) -> pd.DataFrame:
    persisted = load_workflow_from_snowflake(session, reporting_date)
    if persisted.empty:
        draft = prepare_workflow_dataframe(session, None, reporting_date)
        st.session_state.workflow_has_persisted_rows = False
    else:
        draft = prepare_workflow_dataframe(session, persisted, reporting_date)
        st.session_state.workflow_has_persisted_rows = True

    st.session_state.workflow_drafts = draft
    st.session_state.workflow_last_save = draft["Last Saved"].dropna().max() if draft["Last Saved"].notna().any() else pd.NaT
    return draft


def bootstrap_ui_state() -> None:
    defaults = {
        "app_view": "Workflow Dashboard",
        "reporting_date": datetime.now().date(),
        "wf_scope": "All",
        "wf_hide_excluded": True,
        "wf_only_selected": False,
        "wf_search": "",
        "wf_business": [],
        "wf_type": [],
        "wf_buysmart": [],
        "wf_status": [],
        "wf_bucket": [],
        "out_scope": "All",
        "out_search": "",
        "out_business": [],
        "out_type": [],
        "catalog_search": "",
        "catalog_auto": [],
        "catalog_alpha": [],
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def push_notice(level: str, message: str) -> None:
    st.session_state["_app_notice"] = {"level": level, "message": message}


def render_notice() -> None:
    notice = st.session_state.pop("_app_notice", None)
    if not notice:
        return
    level = notice.get("level", "info")
    message = notice.get("message", "")
    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


def display_scalar(value: object, fallback: str = "") -> str:
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.strftime("%b %d, %Y %I:%M %p")
    if isinstance(value, datetime):
        return value.strftime("%b %d, %Y %I:%M %p")
    if isinstance(value, date):
        return value.strftime("%b %d, %Y")
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if math.isnan(value):
            return fallback
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.2f}"
    return clean_text(value)


def format_timestamp(value: object, fallback: str = "Not yet") -> str:
    text = display_scalar(value, fallback="")
    return text or fallback


def ordered_unique_strings(preferred: Iterable[object], current: Iterable[object]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for source in (preferred, current):
        for raw in source:
            value = clean_text(raw)
            if value in seen:
                continue
            seen.add(value)
            ordered.append(value)
    return ordered


def render_status_ribbon(items: Iterable[tuple[str, object]]) -> None:
    chips: list[str] = []
    for label, value in items:
        rendered = display_scalar(value)
        if not rendered:
            continue
        chips.append(f"<span><strong>{html.escape(str(label))}</strong>&nbsp;{html.escape(rendered)}</span>")
    if chips:
        st.markdown(f'<div class="elite-ribbon">{"".join(chips)}</div>', unsafe_allow_html=True)


def render_empty_state(title: str, body: str) -> None:
    st.markdown(
        f"""
        <div class="elite-empty">
            <h4>{html.escape(title)}</h4>
            <p>{html.escape(body)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_neutral_metric(container, label: str, value: object, delta: object, description: str) -> None:
    container.metric(label, value, delta=delta, delta_color="off")
    if description:
        container.caption(description)


def build_upload_state_key(uploaded_file, reporting_date: date) -> str:
    file_name = clean_text(getattr(uploaded_file, "name", "session-upload")) or "session-upload"
    try:
        uploaded_file.seek(0)
        data = uploaded_file.getvalue() if hasattr(uploaded_file, "getvalue") else uploaded_file.read()
        uploaded_file.seek(0)
        signature = hashlib.md5(data).hexdigest()
    except Exception:
        signature = file_name
    return f"upload::{file_name}::{signature}::{reporting_date.isoformat()}"


def ensure_workflow_source_loaded(session, reporting_date: date, uploaded_file) -> pd.DataFrame:
    table_source_key = f"table::{reporting_date.isoformat()}"

    if uploaded_file is not None:
        upload_key = build_upload_state_key(uploaded_file, reporting_date)
        if st.session_state.get("workflow_source_key") != upload_key or "workflow_drafts" not in st.session_state:
            upload_df, upload_sheet_name = load_workflow_sheet(uploaded_file)
            prepared = prepare_workflow_dataframe(
                session,
                upload_df,
                reporting_date,
                batch_id=str(uuid.uuid4()),
                source_file_name=uploaded_file.name,
                source_sheet_name=upload_sheet_name,
            )
            st.session_state.workflow_drafts = prepared
            st.session_state.workflow_last_save = pd.NaT
            st.session_state.workflow_source_key = upload_key
            st.session_state.workflow_has_persisted_rows = False
        return st.session_state.workflow_drafts.copy()

    if st.session_state.get("workflow_source_key") != table_source_key or "workflow_drafts" not in st.session_state:
        initialize_workflow_state_from_table(session, reporting_date)
        st.session_state.workflow_source_key = table_source_key
    return st.session_state.workflow_drafts.copy()


def build_search_mask(df: pd.DataFrame, columns: Iterable[str], query: str) -> pd.Series:
    cleaned_query = clean_text(query).lower()
    if not cleaned_query:
        return pd.Series(True, index=df.index)

    active_columns = [col for col in columns if col in df.columns]
    if not active_columns:
        return pd.Series(True, index=df.index)

    haystack = df[active_columns].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
    mask = pd.Series(True, index=df.index)
    for term in [token for token in cleaned_query.split() if token]:
        mask &= haystack.str.contains(re.escape(term), na=False)
    return mask


def build_rollup(series: pd.Series, label: str, order: list[str] | None = None, fallback: str = "Unspecified") -> pd.DataFrame:
    values = series.fillna(fallback).astype(str).replace("", fallback)
    counts = values.value_counts()
    if order:
        combined_order = list(dict.fromkeys([*order, *counts.index.tolist()]))
        counts = counts.reindex(combined_order, fill_value=0)
    return counts.rename_axis(label).reset_index(name="Rows")


def polish_chart(chart: alt.Chart) -> alt.Chart:
    return (
        chart.configure(background="#ffffff")
        .configure_view(fill="#ffffff", stroke="#e2e8f0")
        .configure_axis(
            labelColor="#0f172a",
            titleColor="#334155",
            gridColor="#e2e8f0",
            domainColor="#cbd5e1",
            tickColor="#cbd5e1",
        )
        .configure_title(color="#0f172a", fontSize=15, fontWeight=700, anchor="start")
        .configure_legend(labelColor="#0f172a", titleColor="#0f172a", orient="right")
    )


def make_horizontal_bar_chart(df: pd.DataFrame, category_col: str, value_col: str, title: str, order: list[str] | None = None) -> alt.Chart:
    chart_height = max(240, min(560, 28 * max(len(df), 1) + 40))
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=8, color="#2859ff")
        .encode(
            x=alt.X(f"{value_col}:Q", title="Rows"),
            y=alt.Y(f"{category_col}:N", sort=order if order else "-x", title=None),
            tooltip=[alt.Tooltip(f"{category_col}:N", title=category_col), alt.Tooltip(f"{value_col}:Q", format=",")],
        )
        .properties(title=title, height=chart_height)
    )
    return polish_chart(chart)


def make_vertical_bar_chart(df: pd.DataFrame, category_col: str, value_col: str, title: str) -> alt.Chart:
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8, color="#0f766e")
        .encode(
            x=alt.X(f"{category_col}:N", sort="-y", title=None, axis=alt.Axis(labelAngle=-20)),
            y=alt.Y(f"{value_col}:Q", title="Rows"),
            tooltip=[alt.Tooltip(f"{category_col}:N", title=category_col), alt.Tooltip(f"{value_col}:Q", format=",")],
        )
        .properties(title=title, height=320)
    )
    return polish_chart(chart)


def make_donut_chart(df: pd.DataFrame, category_col: str, value_col: str, title: str) -> alt.Chart:
    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=70, outerRadius=118)
        .encode(
            theta=alt.Theta(f"{value_col}:Q"),
            color=alt.Color(
                f"{category_col}:N",
                legend=alt.Legend(title=None),
                scale=alt.Scale(range=["#2859ff", "#0f766e", "#f59e0b", "#dc2626", "#7c3aed", "#0891b2", "#64748b", "#db2777"]),
            ),
            tooltip=[alt.Tooltip(f"{category_col}:N", title=category_col), alt.Tooltip(f"{value_col}:Q", format=",")],
        )
        .properties(title=title, height=320)
    )
    return polish_chart(chart)


def render_pair_table(pairs: list[tuple[str, object]]) -> None:
    rows = [{"Field": label, "Value": display_scalar(value, fallback="-") or "-"} for label, value in pairs]
    st.table(pd.DataFrame(rows))


def render_shell(session) -> tuple[str, date, object]:
    hero_col, command_col = st.columns([1.65, 1.0], gap="large", vertical_alignment="top")

    with command_col:
        st.markdown("##### Command deck")
        st.caption("Switch workspace, lock the reporting date, and optionally override Snowflake with a session workbook.")

        app_view = st.segmented_control(
            "Workspace",
            options=WORKSPACE_OPTIONS,
            format_func=lambda option: WORKSPACE_META[option]["nav"],
            key="app_view",
            width="stretch",
        )
        app_view = app_view or "Workflow Dashboard"

        reporting_date = st.date_input("Reporting as of", key="reporting_date")
        if isinstance(reporting_date, tuple):
            reporting_date = reporting_date[0]
        reporting_date = reporting_date or datetime.now().date()

        with st.expander("Session workbook override", expanded=False):
            uploaded_file = st.file_uploader(
                "Upload daily PRF / SORF / SRF workbook",
                type=["xlsx"],
                key="session_workbook",
                help="When a workbook is loaded, it becomes the active working set for this browser session.",
            )
            if uploaded_file is not None:
                st.success(f"{uploaded_file.name} is active for this session.")
            else:
                st.caption("No workbook override is active. Warehouse-backed rows are used for the selected date.")

    with hero_col:
        meta = WORKSPACE_META.get(app_view, WORKSPACE_META["Workflow Dashboard"])
        source_chip = f"Workbook override: {clean_text(getattr(uploaded_file, 'name', ''))}" if uploaded_file is not None else "Snowflake-backed working set"
        st.markdown(
            f"""
            <div class="elite-hero">
                <div class="elite-kicker">{html.escape(meta['kicker'])}</div>
                <h1>{html.escape(meta['title'])}</h1>
                <p>{html.escape(meta['description'])}</p>
                <div class="elite-chip-row">
                    <span class="elite-chip">Report date: {html.escape(display_scalar(reporting_date))}</span>
                    <span class="elite-chip">{html.escape(source_chip)}</span>
                    <span class="elite-chip">Light theme controls</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    return app_view, reporting_date, uploaded_file


def render_sidebar(session, reporting_date: date, uploaded_file) -> None:
    with st.sidebar:
        st.markdown("### Workspace guide")
        st.caption("The rule engine and Snowflake contract stay intact while the work surface stays readable.")
        st.markdown(
            """
            **Workflow Dashboard** - run deterministic rules, edit decisions, and save.  
            **Outcome Reporting** - produce a cleaner outcome summary.  
            **Rule Catalog** - inspect automation coverage and harvested rules.
            """
        )
        render_status_ribbon(
            [
                ("Report date", reporting_date),
                ("Source", uploaded_file.name if uploaded_file is not None else "Snowflake"),
            ]
        )

        with st.expander("Snowflake session", expanded=False):
            st.write(get_session_context(session) or {"status": "Unavailable"})
            st.write(
                {
                    "workflow_table": resolve_table_name(session, TABLE_WORKFLOW) or "[not resolved]",
                    "rule_catalog": resolve_table_name(session, TABLE_RULE_CATALOG) or "[not resolved]",
                    "local_vendor": resolve_table_name(session, TABLE_LOCAL_VENDOR) or "[not resolved]",
                    "disallowed_min": resolve_table_name(session, TABLE_DISALLOWED_MIN) or "[not resolved]",
                    "allowlist": resolve_table_name(session, TABLE_ALLOWLIST) or "[not resolved]",
                }
            )


def reset_workflow_filters() -> None:
    st.session_state.update(
        {
            "wf_scope": "All",
            "wf_hide_excluded": True,
            "wf_only_selected": False,
            "wf_search": "",
            "wf_business": [],
            "wf_type": [],
            "wf_buysmart": [],
            "wf_status": [],
            "wf_bucket": [],
        }
    )


def reset_outcome_filters() -> None:
    st.session_state.update({"out_scope": "All", "out_search": "", "out_business": [], "out_type": []})


def reset_catalog_filters() -> None:
    st.session_state.update({"catalog_search": "", "catalog_auto": [], "catalog_alpha": []})


def build_workflow_filter_mask(df: pd.DataFrame) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    scope = st.session_state.get("wf_scope", "All")

    if scope == "Needs Review":
        mask &= df["Needs Review"].fillna(False)
    elif scope == "Approved":
        mask &= df["Buysmart Action"].fillna("").astype(str).str.lower().eq("approved")
    elif scope == "Denied":
        mask &= df["Buysmart Action"].fillna("").astype(str).str.lower().eq("denied")
    elif scope == "Assigned":
        mask &= df["Buysmart Action"].fillna("").astype(str).str.lower().isin(["assigned", ""])
    elif scope == "Excluded":
        mask &= df["Excluded"].fillna(False)

    if scope != "Excluded" and st.session_state.get("wf_hide_excluded", True):
        mask &= ~df["Excluded"].fillna(False)
    if st.session_state.get("wf_only_selected", False):
        mask &= df["Selected"].fillna(False)

    for state_key, column in [
        ("wf_business", "Business"),
        ("wf_type", "Type"),
        ("wf_buysmart", "Buysmart Action"),
        ("wf_status", "Status"),
        ("wf_bucket", "Request Bucket"),
    ]:
        values = st.session_state.get(state_key, [])
        if values:
            mask &= df[column].fillna("").isin(values)

    mask &= build_search_mask(df, WORKFLOW_SEARCH_COLUMNS, st.session_state.get("wf_search", ""))
    return mask


def render_workflow_filter_bar(df: pd.DataFrame) -> pd.DataFrame:
    search_col, lens_col, filter_col = st.columns([1.55, 1.15, 1.0], gap="small", vertical_alignment="bottom")

    with search_col:
        st.text_input("Search", placeholder="Case, vendor, description, DIN, MIN, notes...", key="wf_search")

    with lens_col:
        st.segmented_control("Queue lens", options=QUEUE_LENS_OPTIONS, key="wf_scope", width="stretch")

    with filter_col:
        with st.popover("Advanced filters"):
            st.toggle("Hide excluded", key="wf_hide_excluded")
            st.toggle("Only selected", key="wf_only_selected")
            st.multiselect("Business", options=sorted(v for v in df["Business"].dropna().astype(str).unique() if v), key="wf_business")
            st.multiselect("Request type", options=sorted(v for v in df["Type"].dropna().astype(str).unique() if v), key="wf_type")
            st.multiselect("BuySmart action", options=ordered_unique_strings(KNOWN_BUYSMART_OPTIONS, df["Buysmart Action"].dropna().astype(str).tolist()), key="wf_buysmart")
            st.multiselect("Status", options=ordered_unique_strings(KNOWN_STATUS_OPTIONS, df["Status"].dropna().astype(str).tolist()), key="wf_status")
            st.multiselect("Request bucket", options=ordered_unique_strings(REQUEST_BUCKET_ORDER, df["Request Bucket"].dropna().astype(str).tolist()), key="wf_bucket")
            st.button("Reset filters", on_click=reset_workflow_filters)

    filtered_df = df.loc[build_workflow_filter_mask(df)].copy()
    render_status_ribbon(
        [
            ("Visible", f"{len(filtered_df):,} of {len(df):,}"),
            ("Focused rows", int(filtered_df["Selected"].fillna(False).sum())),
            ("Lens", st.session_state.get("wf_scope", "All")),
            ("Hidden excluded", "Yes" if st.session_state.get("wf_hide_excluded", True) else "No"),
        ]
    )
    return filtered_df


def render_workflow_overview_metrics(full_df: pd.DataFrame, visible_df: pd.DataFrame) -> None:
    total_rows = len(full_df)
    visible_rows = len(visible_df)
    review_rows = int(full_df["Needs Review"].fillna(False).sum())
    excluded_rows = int(full_df["Excluded"].fillna(False).sum())
    approved_rows = int(full_df["Buysmart Action"].fillna("").astype(str).str.lower().eq("approved").sum())
    denied_rows = int(full_df["Buysmart Action"].fillna("").astype(str).str.lower().eq("denied").sum())
    assigned_rows = int(full_df["Buysmart Action"].fillna("").astype(str).str.lower().isin(["assigned", ""]).sum())
    pct = lambda value: f"{(value / total_rows):.0%}" if total_rows else "0%"

    metric_cols = st.columns(6, gap="small")
    render_neutral_metric(metric_cols[0], "Working set", f"{total_rows:,}", f"{visible_rows:,}", "visible after filters")
    render_neutral_metric(metric_cols[1], "Needs review", f"{review_rows:,}", pct(review_rows), "of working set")
    render_neutral_metric(metric_cols[2], "Approved", f"{approved_rows:,}", pct(approved_rows), "BuySmart approved")
    render_neutral_metric(metric_cols[3], "Denied", f"{denied_rows:,}", pct(denied_rows), "BuySmart denied")
    render_neutral_metric(metric_cols[4], "Assigned", f"{assigned_rows:,}", pct(assigned_rows), "still in queue")
    render_neutral_metric(metric_cols[5], "Excluded", f"{excluded_rows:,}", pct(excluded_rows), "removed from queue")


def build_editor_column_config(df: pd.DataFrame) -> dict[str, object]:
    action_options = ordered_unique_strings(KNOWN_ACTION_OPTIONS, df["ACTION"].dropna().astype(str).tolist())
    if_stock_options = ordered_unique_strings(KNOWN_IF_STOCK_OPTIONS, df["If In Stock: Action"].dropna().astype(str).tolist())
    buysmart_options = ordered_unique_strings(KNOWN_BUYSMART_OPTIONS, df["Buysmart Action"].dropna().astype(str).tolist())
    status_options = ordered_unique_strings(KNOWN_STATUS_OPTIONS, df["Status"].dropna().astype(str).tolist())
    return {
        "Selected": st.column_config.CheckboxColumn("Focus", help="Pin rows to the focused review panel."),
        "Business": st.column_config.TextColumn("Business", width="small"),
        "Type": st.column_config.TextColumn("Type", width="small"),
        "Case#": st.column_config.TextColumn("Case", width="medium"),
        "Division": st.column_config.TextColumn("Division", width="medium"),
        "Vendor": st.column_config.TextColumn("Vendor", width="large"),
        "Description": st.column_config.TextColumn("Description", width="large"),
        "DIN": st.column_config.TextColumn("DIN", width="small"),
        "MIN": st.column_config.TextColumn("MIN", width="small"),
        "One-Time or Permanent": st.column_config.TextColumn("Term", width="small"),
        "Meets Criteria": st.column_config.NumberColumn("Meets Criteria", format="0.0%"),
        "ACTION": st.column_config.SelectboxColumn("Action", options=action_options, width="medium"),
        "If In Stock: Action": st.column_config.SelectboxColumn("If In Stock", options=if_stock_options, width="small"),
        "Buysmart Action": st.column_config.SelectboxColumn("BuySmart", options=buysmart_options, width="small"),
        "Needs Review": st.column_config.CheckboxColumn("Review"),
        "Validation Status": st.column_config.TextColumn("Validation", width="large"),
        "Analyst Notes": st.column_config.TextColumn("Analyst Notes", width="large"),
        "Excluded": st.column_config.CheckboxColumn("Excluded"),
        "Excluded Reason": st.column_config.TextColumn("Excluded Reason", width="medium"),
        "Assignment": st.column_config.TextColumn("Assignment", width="medium"),
        "Status": st.column_config.SelectboxColumn("Status", options=status_options, width="small"),
        "Rule Applied": st.column_config.TextColumn("Rule Applied", width="medium"),
        "Last Sync": st.column_config.DatetimeColumn("Last Sync", disabled=True),
        "Last Saved": st.column_config.DatetimeColumn("Last Saved", disabled=True),
    }


def render_selected_rows_panel(workflow_df: pd.DataFrame) -> None:
    selected_df = workflow_df[workflow_df["Selected"].fillna(False)].copy()
    with st.expander("Focused record review", expanded=bool(len(selected_df))):
        if selected_df.empty:
            render_empty_state(
                "No focused rows yet",
                "Use the Focus checkbox in the workbench to pin one or more rows here for deeper review.",
            )
            return

        if len(selected_df) > 1:
            st.caption(f"{len(selected_df):,} rows are currently focused.")
            st.dataframe(
                selected_df[["Business", "Type", "Case#", "Vendor", "Description", "ACTION", "Buysmart Action", "Needs Review", "Status"]],
                use_container_width=True,
                hide_index=True,
                height=320,
            )
            return

        row = selected_df.iloc[0]
        title = clean_text(row.get("Description")) or "Selected request"
        subtitle_parts = [clean_text(row.get("Business")), clean_text(row.get("Type")), clean_text(row.get("Case#"))]
        st.markdown(f"##### {title}")
        st.caption(" | ".join(part for part in subtitle_parts if part))
        render_status_ribbon(
            [
                ("Action", row.get("ACTION")),
                ("BuySmart", row.get("Buysmart Action")),
                ("Status", row.get("Status")),
                ("Needs review", bool(row.get("Needs Review", False))),
            ]
        )

        left, right = st.columns([1.05, 0.95], gap="large")
        with left:
            meets_criteria = pd.to_numeric(pd.Series([row.get("Meets Criteria")]), errors="coerce").iloc[0]
            render_pair_table(
                [
                    ("Vendor", row.get("Vendor")),
                    ("Division", row.get("Division")),
                    ("Sector", row.get("Sector")),
                    ("Unit", clean_text(row.get("Unit Name")) or clean_text(row.get("Unit Number"))),
                    ("DIN", row.get("DIN")),
                    ("MIN", row.get("MIN")),
                    ("Manufacturer", row.get("Manufacturer")),
                    ("Brand", row.get("Brand")),
                    ("Parent Category", row.get("Parent Category")),
                    ("Sub Category", row.get("Sub Category")),
                    ("Request term", row.get("One-Time or Permanent")),
                    ("Meets Criteria", f"{meets_criteria:.1%}" if pd.notna(meets_criteria) else "-"),
                ]
            )
        with right:
            render_pair_table(
                [
                    ("Rule Applied", row.get("Rule Applied")),
                    ("If In Stock", row.get("If In Stock: Action")),
                    ("Validation", row.get("Validation Status")),
                    ("Excluded", bool(row.get("Excluded", False))),
                    ("Excluded Reason", row.get("Excluded Reason")),
                    ("Assignment", row.get("Assignment")),
                    ("Outcome", row.get("Outcome Reporting")),
                    ("Last Sync", row.get("Last Sync")),
                    ("Last Saved", row.get("Last Saved")),
                ]
            )
            if clean_text(row.get("Analyst Notes")):
                st.markdown("##### Analyst notes")
                st.write(clean_text(row.get("Analyst Notes")))


def render_workbench_editor(workflow_df: pd.DataFrame, filtered_df: pd.DataFrame) -> None:
    if filtered_df.empty:
        render_empty_state(
            "No rows match the current filters",
            "Broaden the lens, clear one or more filters, or reset the view.",
        )
        return

    editor_source = filtered_df[["workflow_request_id", *WORKBENCH_COLUMNS]].copy().set_index("workflow_request_id", drop=True)
    edited_visible = st.data_editor(
        editor_source,
        height=680,
        hide_index=True,
        column_order=WORKBENCH_COLUMNS,
        column_config=build_editor_column_config(workflow_df),
        disabled=[col for col in WORKBENCH_COLUMNS if col not in WORKBENCH_EDITABLE_COLUMNS],
        key="workflow_editor",
    )

    st.session_state.workflow_drafts = sync_visible_editor_changes(workflow_df, edited_visible.reset_index())
    st.caption(f"Showing {len(filtered_df):,} rows. Focused rows: {int(st.session_state.workflow_drafts['Selected'].fillna(False).sum()):,}.")
    render_selected_rows_panel(st.session_state.workflow_drafts.copy())


def render_workflow_insights(session, workflow_df: pd.DataFrame, filtered_df: pd.DataFrame) -> None:
    if filtered_df.empty:
        render_empty_state(
            "Insights are waiting on visible data",
            "The current filter set hides all rows. Expand the lens to see request mix and outcomes.",
        )
        return

    request_rollup = build_rollup(filtered_df["Request Bucket"], "Request Bucket", REQUEST_BUCKET_ORDER, fallback="Unbucketed")
    outcome_rollup = build_rollup(filtered_df["Outcome Reporting"], "Outcome Reporting", OUTCOME_REPORT_ORDER, fallback="assigned")
    business_rollup = build_rollup(filtered_df["Business"], "Business").head(8)
    review_reason_rollup = build_rollup(
        filtered_df.loc[filtered_df["Needs Review"].fillna(False), "Validation Status"],
        "Validation Status",
        fallback="Needs analyst judgment",
    )

    left, right = st.columns([1.05, 0.95], gap="large")
    with left:
        st.altair_chart(make_horizontal_bar_chart(request_rollup, "Request Bucket", "Rows", "Request mix", REQUEST_BUCKET_ORDER), use_container_width=True, theme=None)
    with right:
        st.altair_chart(make_donut_chart(outcome_rollup, "Outcome Reporting", "Rows", "Outcome distribution"), use_container_width=True, theme=None)

    lower_left, lower_right = st.columns([1.0, 1.0], gap="large")
    with lower_left:
        st.altair_chart(make_vertical_bar_chart(business_rollup, "Business", "Rows", "Top businesses in view"), use_container_width=True, theme=None)
    with lower_right:
        st.markdown("##### Rule coverage snapshot")
        st.table(load_rule_catalog_summary(session))

    st.markdown("##### Review concentration")
    if review_reason_rollup.empty:
        render_empty_state("No active review reasons", "The visible rows currently have no validation or review flags.")
    else:
        st.table(review_reason_rollup)


def render_review_queue(workflow_df: pd.DataFrame, filtered_df: pd.DataFrame) -> None:
    review_df = filtered_df[filtered_df["Needs Review"].fillna(False)].copy()
    excluded_df = filtered_df[filtered_df["Excluded"].fillna(False)].copy()

    top_cols = st.columns(3, gap="small")
    total_visible = len(filtered_df)
    pct = lambda count: f"{(count / total_visible):.0%}" if total_visible else "0%"
    render_neutral_metric(top_cols[0], "Review queue", f"{len(review_df):,}", pct(len(review_df)), "of visible rows")
    render_neutral_metric(top_cols[1], "Excluded in view", f"{len(excluded_df):,}", pct(len(excluded_df)), "of visible rows")
    render_neutral_metric(
        top_cols[2],
        "Focused review rows",
        f"{int(workflow_df['Selected'].fillna(False).sum()):,}",
        f"{int(review_df['Selected'].fillna(False).sum()):,}",
        "selected for follow-up",
    )

    left, right = st.columns([1.25, 0.75], gap="large")
    with left:
        st.markdown("##### Needs review queue")
        if review_df.empty:
            render_empty_state("No visible rows need review", "The current visible queue has no active review flags.")
        else:
            st.dataframe(review_df[REVIEW_COLUMNS], use_container_width=True, hide_index=True, height=420)

    with right:
        st.markdown("##### Review reasons")
        reason_rollup = build_rollup(review_df["Validation Status"], "Validation Status", fallback="Needs analyst judgment")
        st.table(reason_rollup) if not reason_rollup.empty else render_empty_state("No review reasons captured", "Validation messages will appear here.")

        st.markdown("##### Exclusion reasons")
        exclusion_rollup = build_rollup(excluded_df["Excluded Reason"], "Excluded Reason", fallback="Excluded")
        st.table(exclusion_rollup) if not exclusion_rollup.empty else render_empty_state("No exclusions in view", "Excluded rows will appear here.")


def render_workflow_dashboard(session, reporting_date: date, uploaded_file) -> None:
    refs = load_reference_data(session)
    workflow_df = ensure_workflow_source_loaded(session, reporting_date, uploaded_file)
    workflow_table = resolve_table_name(session, TABLE_WORKFLOW)

    last_sync = pd.to_datetime(workflow_df["Last Sync"], errors="coerce").dropna().max() if "Last Sync" in workflow_df.columns else pd.NaT
    last_saved = pd.to_datetime(workflow_df["Last Saved"], errors="coerce").dropna().max() if "Last Saved" in workflow_df.columns else pd.NaT
    automation_state = "Rules applied" if workflow_df["Rule Applied"].fillna("").astype(str).str.strip().ne("").any() else "Awaiting rule run"
    source_label = uploaded_file.name if uploaded_file is not None else (workflow_table or "Sample / unresolved source")
    persistence_label = "Session workbook" if uploaded_file is not None else ("Snowflake rows" if st.session_state.get("workflow_has_persisted_rows", False) else "Sample draft")

    render_status_ribbon(
        [
            ("Source", source_label),
            ("Persistence", persistence_label),
            ("Last sync", format_timestamp(last_sync)),
            ("Last save", format_timestamp(last_saved)),
            ("Automation", automation_state),
        ]
    )

    action_cols = st.columns([1.2, 1.0, 1.0, 1.0], gap="small")
    run_rules_clicked = action_cols[0].button("Run harvested rules", type="primary")
    save_clicked = action_cols[1].button("Save to Snowflake", disabled=workflow_table is None or session is None)
    reload_clicked = action_cols[2].button("Reload warehouse", disabled=uploaded_file is not None or session is None)

    export_df = workflow_df.drop(
        columns=[
            c
            for c in [
                "workflow_request_id",
                "batch_id",
                "source_file_name",
                "source_sheet_name",
                "source_row_number",
                "reporting_date",
                "is_active",
            ]
            if c in workflow_df.columns
        ],
        errors="ignore",
    )
    action_cols[3].download_button(
        "Export CSV",
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name=f"rules_driven_workflow_{reporting_date:%Y%m%d}.csv",
        mime="text/csv",
    )

    if run_rules_clicked:
        st.session_state.workflow_drafts = run_harvested_alpha_rules(workflow_df, refs)
        push_notice("success", "Harvested Alpha rules were applied. Guided rows remain flagged for analyst review.")
        st.rerun()

    if reload_clicked and uploaded_file is None:
        initialize_workflow_state_from_table(session, reporting_date)
        st.session_state.workflow_source_key = f"table::{reporting_date.isoformat()}"
        push_notice("info", "The working set was reloaded from Snowflake for the selected reporting date.")
        st.rerun()

    workflow_df = st.session_state.workflow_drafts.copy()
    filtered_df = render_workflow_filter_bar(workflow_df)
    render_workflow_overview_metrics(workflow_df, filtered_df)

    workbench_tab, insights_tab, review_tab = st.tabs(["Workbench", "Insights", "Review Queue"])
    with workbench_tab:
        render_workbench_editor(workflow_df, filtered_df)
    with insights_tab:
        render_workflow_insights(session, workflow_df, filtered_df)
    with review_tab:
        render_review_queue(workflow_df, filtered_df)

    if save_clicked:
        to_save = st.session_state.workflow_drafts.copy()
        now_ts = pd.Timestamp(datetime.now())
        to_save["Last Saved"] = now_ts
        try:
            rows_written = merge_workflow_to_snowflake(session, to_save)
        except Exception as exc:
            st.error(f"Save failed: {exc}")
        else:
            st.session_state.workflow_drafts = to_save
            st.session_state.workflow_last_save = now_ts
            st.session_state.workflow_has_persisted_rows = True
            push_notice("success", f"Saved {rows_written:,} row(s) to Snowflake.")
            st.rerun()


def build_outcome_filter_mask(df: pd.DataFrame) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    scope = st.session_state.get("out_scope", "All")
    if scope == "Approved":
        mask &= df["Outcome Reporting"].fillna("").astype(str).str.lower().isin(["approved", "1x approved"])
    elif scope == "Denied":
        mask &= df["Outcome Reporting"].fillna("").astype(str).str.lower().eq("denied")
    elif scope == "Use Right / Alt":
        mask &= df["Outcome Reporting"].fillna("").astype(str).str.lower().isin(["use right", "find alt first"])
    elif scope == "Needs Review":
        mask &= df["Needs Review"].fillna(False)

    for state_key, column in [("out_business", "Business"), ("out_type", "Type")]:
        values = st.session_state.get(state_key, [])
        if values:
            mask &= df[column].fillna("").isin(values)

    mask &= build_search_mask(df, OUTCOME_SEARCH_COLUMNS, st.session_state.get("out_search", ""))
    return mask


def render_outcome_filter_bar(df: pd.DataFrame) -> pd.DataFrame:
    search_col, lens_col, filter_col = st.columns([1.55, 1.15, 1.0], gap="small", vertical_alignment="bottom")
    with search_col:
        st.text_input("Search outcomes", placeholder="Case, vendor, description, action, notes...", key="out_search")
    with lens_col:
        st.segmented_control("Outcome lens", options=OUTCOME_LENS_OPTIONS, key="out_scope", width="stretch")
    with filter_col:
        with st.popover("Advanced filters"):
            st.multiselect("Business", options=sorted(v for v in df["Business"].dropna().astype(str).unique() if v), key="out_business")
            st.multiselect("Request type", options=sorted(v for v in df["Type"].dropna().astype(str).unique() if v), key="out_type")
            st.button("Reset filters", on_click=reset_outcome_filters)

    filtered_df = df.loc[build_outcome_filter_mask(df)].copy()
    render_status_ribbon(
        [
            ("Visible", f"{len(filtered_df):,} of {len(df):,}"),
            ("Lens", st.session_state.get("out_scope", "All")),
            ("Needs review", int(filtered_df["Needs Review"].fillna(False).sum())),
        ]
    )
    return filtered_df


def render_outcome_reporting(session, reporting_date: date, uploaded_file) -> None:
    workflow_df = ensure_workflow_source_loaded(session, reporting_date, uploaded_file)
    preview_mode = False
    if "Request Bucket" not in workflow_df.columns or workflow_df["Request Bucket"].fillna("").eq("").all():
        workflow_df = run_harvested_alpha_rules(workflow_df, load_reference_data(session))
        preview_mode = True

    render_status_ribbon(
        [
            ("Source", "Current workflow draft"),
            ("Mode", "Preview rule run" if preview_mode else "Current draft"),
            ("Rows", len(workflow_df)),
        ]
    )

    filtered_df = render_outcome_filter_bar(workflow_df)
    outcome_rollup_all = build_rollup(workflow_df["Outcome Reporting"], "Outcome Reporting", OUTCOME_REPORT_ORDER, fallback="assigned")
    approved_count = int(outcome_rollup_all.loc[outcome_rollup_all["Outcome Reporting"].isin(["approved", "1x approved"]), "Rows"].sum())
    denied_count = int(outcome_rollup_all.loc[outcome_rollup_all["Outcome Reporting"] == "denied", "Rows"].sum())
    alt_count = int(outcome_rollup_all.loc[outcome_rollup_all["Outcome Reporting"].isin(["use right", "find alt first"]), "Rows"].sum())
    review_count = int(workflow_df["Needs Review"].fillna(False).sum())
    total_rows = len(workflow_df)
    pct = lambda count: f"{(count / total_rows):.0%}" if total_rows else "0%"

    metric_cols = st.columns(4, gap="small")
    render_neutral_metric(metric_cols[0], "Approved", f"{approved_count:,}", pct(approved_count), "approved outcomes")
    render_neutral_metric(metric_cols[1], "Denied", f"{denied_count:,}", pct(denied_count), "denied outcomes")
    render_neutral_metric(metric_cols[2], "Use Right / Alt", f"{alt_count:,}", pct(alt_count), "redirected outcomes")
    render_neutral_metric(metric_cols[3], "Needs review", f"{review_count:,}", pct(review_count), "still unresolved")

    export_df = filtered_df[OUTCOME_DETAIL_COLUMNS].copy() if not filtered_df.empty else pd.DataFrame(columns=OUTCOME_DETAIL_COLUMNS)
    st.download_button(
        "Download outcome extract",
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name=f"outcome_reporting_{reporting_date:%Y%m%d}.csv",
        mime="text/csv",
    )

    summary_tab, detail_tab = st.tabs(["Summary", "Records"])
    with summary_tab:
        if filtered_df.empty:
            render_empty_state("No rows match the current outcome filters", "Broaden the current outcome lens or reset the filters.")
        else:
            outcome_rollup = build_rollup(filtered_df["Outcome Reporting"], "Outcome Reporting", OUTCOME_REPORT_ORDER, fallback="assigned")
            request_rollup = build_rollup(filtered_df["Request Bucket"], "Request Bucket", REQUEST_BUCKET_ORDER, fallback="Unbucketed")
            business_rollup = build_rollup(filtered_df["Business"], "Business").head(8)
            left, right = st.columns([0.95, 1.05], gap="large")
            with left:
                st.altair_chart(make_donut_chart(outcome_rollup, "Outcome Reporting", "Rows", "Outcome distribution"), use_container_width=True, theme=None)
            with right:
                st.altair_chart(make_horizontal_bar_chart(request_rollup, "Request Bucket", "Rows", "Request buckets", REQUEST_BUCKET_ORDER), use_container_width=True, theme=None)
            st.altair_chart(make_vertical_bar_chart(business_rollup, "Business", "Rows", "Top businesses in view"), use_container_width=True, theme=None)

    with detail_tab:
        if filtered_df.empty:
            render_empty_state("No records to display", "Adjust the current filters to inspect the row-level outcome detail table.")
        else:
            st.dataframe(filtered_df[OUTCOME_DETAIL_COLUMNS], use_container_width=True, hide_index=True, height=620)


def filter_rule_catalog(rule_catalog: pd.DataFrame) -> pd.DataFrame:
    if rule_catalog.empty:
        return rule_catalog

    filtered = rule_catalog.copy()
    auto_col = next((col for col in rule_catalog.columns if str(col).upper() == "AUTOMATION_CANDIDATE"), None)
    alpha_col = next((col for col in rule_catalog.columns if str(col).upper() == "ALPHA_RECOMMENDATION"), None)

    auto_filter = st.session_state.get("catalog_auto", [])
    if auto_col and auto_filter:
        filtered = filtered[filtered[auto_col].fillna("").astype(str).isin(auto_filter)]

    alpha_filter = st.session_state.get("catalog_alpha", [])
    if alpha_col and alpha_filter:
        filtered = filtered[filtered[alpha_col].fillna("").astype(str).isin(alpha_filter)]

    search_text = st.session_state.get("catalog_search", "")
    if clean_text(search_text):
        filtered = filtered.loc[build_search_mask(filtered, filtered.columns.tolist(), search_text)]
    return filtered.copy()


def render_catalog_filters(rule_catalog: pd.DataFrame) -> pd.DataFrame:
    search_col, filter_col = st.columns([1.65, 1.0], gap="small", vertical_alignment="bottom")
    with search_col:
        st.text_input("Search rules", placeholder="Rule id, condition, recommendation, exception...", key="catalog_search")
    with filter_col:
        with st.popover("Catalog filters"):
            auto_col = next((col for col in rule_catalog.columns if str(col).upper() == "AUTOMATION_CANDIDATE"), None)
            alpha_col = next((col for col in rule_catalog.columns if str(col).upper() == "ALPHA_RECOMMENDATION"), None)
            if auto_col:
                st.multiselect("Automation Candidate", options=sorted(v for v in rule_catalog[auto_col].dropna().astype(str).unique() if v), key="catalog_auto")
            if alpha_col:
                st.multiselect("Alpha Recommendation", options=sorted(v for v in rule_catalog[alpha_col].dropna().astype(str).unique() if v), key="catalog_alpha")
            st.button("Reset filters", on_click=reset_catalog_filters)

    filtered = filter_rule_catalog(rule_catalog)
    render_status_ribbon(
        [
            ("Visible rules", f"{len(filtered):,} of {len(rule_catalog):,}"),
            ("Search", clean_text(st.session_state.get("catalog_search", "")) or "None"),
        ]
    )
    return filtered


def render_rule_catalog(session) -> None:
    summary = load_rule_catalog_summary(session)
    rule_catalog = load_table_if_exists(session, TABLE_RULE_CATALOG)

    total_rules = int(summary.loc[summary["Metric"] == "Total harvested rules", "Count"].sum())
    auto_yes = int(summary.loc[summary["Metric"] == "Automation Candidate = Yes", "Count"].sum())
    auto_partial = int(summary.loc[summary["Metric"] == "Automation Candidate = Partial", "Count"].sum())
    auto_no = int(summary.loc[summary["Metric"] == "Automation Candidate = No", "Count"].sum())
    alpha_rules = int(summary.loc[summary["Metric"] == "Alpha Recommendation = Alpha", "Count"].sum())
    guided_rules = int(summary.loc[summary["Metric"] == "Alpha Recommendation = Guided", "Count"].sum())
    future_rules = int(summary.loc[summary["Metric"] == "Alpha Recommendation = Future", "Count"].sum())
    pct = lambda count: f"{(count / total_rules):.0%}" if total_rules else "0%"

    metric_cols = st.columns(6, gap="small")
    render_neutral_metric(metric_cols[0], "Harvested rules", f"{total_rules:,}", f"{auto_yes:,}", "automation-ready")
    render_neutral_metric(metric_cols[1], "Automation = Yes", f"{auto_yes:,}", pct(auto_yes), "of catalog")
    render_neutral_metric(metric_cols[2], "Automation = Partial", f"{auto_partial:,}", pct(auto_partial), "of catalog")
    render_neutral_metric(metric_cols[3], "Automation = No", f"{auto_no:,}", pct(auto_no), "of catalog")
    render_neutral_metric(metric_cols[4], "Alpha", f"{alpha_rules:,}", pct(alpha_rules), "recommended now")
    render_neutral_metric(metric_cols[5], "Guided / Future", f"{guided_rules + future_rules:,}", pct(guided_rules + future_rules), "non-alpha work")

    if rule_catalog.empty:
        st.markdown("##### Rule catalog overview")
        st.table(summary)
        render_empty_state(
            "The rule catalog table is not populated",
            "Seed the rule catalog table to browse harvested rule inventory and automation recommendations.",
        )
        return

    filtered_catalog = render_catalog_filters(rule_catalog)
    st.download_button(
        "Download filtered catalog",
        data=filtered_catalog.to_csv(index=False).encode("utf-8"),
        file_name="rule_catalog_filtered.csv",
        mime="text/csv",
    )

    overview_tab, inventory_tab = st.tabs(["Overview", "Rule inventory"])
    with overview_tab:
        auto_rollup = pd.DataFrame({"Automation Candidate": ["Yes", "Partial", "No"], "Rows": [auto_yes, auto_partial, auto_no]})
        alpha_rollup = pd.DataFrame({"Alpha Recommendation": ["Alpha", "Guided", "Future"], "Rows": [alpha_rules, guided_rules, future_rules]})
        left, right = st.columns(2, gap="large")
        with left:
            st.altair_chart(make_vertical_bar_chart(auto_rollup, "Automation Candidate", "Rows", "Automation coverage"), use_container_width=True, theme=None)
        with right:
            st.altair_chart(make_vertical_bar_chart(alpha_rollup, "Alpha Recommendation", "Rows", "Alpha recommendation mix"), use_container_width=True, theme=None)
        st.markdown("##### Summary table")
        st.table(summary)

    with inventory_tab:
        if filtered_catalog.empty:
            render_empty_state("No catalog rows match the current filters", "Clear or broaden the current catalog filters.")
        else:
            st.dataframe(filtered_catalog, use_container_width=True, hide_index=True, height=680)


def main() -> None:
    bootstrap_ui_state()
    inject_global_styles()
    session = get_session()
    app_view, reporting_date, uploaded_file = render_shell(session)
    render_sidebar(session, reporting_date, uploaded_file)
    render_notice()

    if app_view == "Outcome Reporting":
        render_outcome_reporting(session, reporting_date, uploaded_file)
    elif app_view == "Rule Catalog":
        render_rule_catalog(session)
    else:
        render_workflow_dashboard(session, reporting_date, uploaded_file)


if __name__ == "__main__":
    main()
