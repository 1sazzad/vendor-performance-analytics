# ETL Process in This Project

ETL means Extract, Transform, and Load.

In this repository, the workflow is:

1. Extract: read raw vendor CSV files from the data folder.
2. Transform: clean and aggregate records into business-ready metrics.
3. Load: save both raw and curated data into SQLite for analysis.

Practical implementation:

- `ingestion_db.py` performs extraction and loading of raw CSV files into SQLite tables.
- `get_vendor_summary.py` transforms raw tables into `vendor_sales_summary`.
- `app.py` presents those curated metrics in a Streamlit dashboard.

The result is a repeatable analytics pipeline that is simple to run and easy to inspect.
