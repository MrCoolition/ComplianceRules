# Compliance Rules

Snowflake Streamlit app for reviewing analyst rule decisions, running harvested Alpha rules, and exporting outcome reporting.

## Snowflake setup

1. Push this repository to `MrCoolition/ComplianceRules`.
2. In Snowflake Streamlit, use **Connect Git repository**.
3. Select the database/schema that contains the Snowflake Git repository object.
4. Choose this repo folder and use `streamlit_app.py` as the Streamlit entry point.

The `.streamlit/config.toml` and `snowpark_streamlit_fix.py` files force a light theme for Snowflake-hosted Streamlit controls, menus, date pickers, popovers, tables, and charts. That fixes the blacked-out segmented controls, search/date inputs, advanced-filter menus, and chart panels seen in Snowflake.
