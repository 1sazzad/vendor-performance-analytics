# Vendor Performance Analytics

A lightweight analytics project for ingesting vendor CSV extracts into SQLite, transforming them into a clean vendor summary table, and supporting exploratory work for engineers and data analysts.

## Project Goals

- Load raw procurement, pricing, invoice, and sales data into a local analytical database.
- Build a denormalized vendor summary table that is easy to query and visualize.
- Standardize business metrics such as gross profit, profit margin, stock turnover, and sales-to-purchase ratio.
- Keep the project small, reproducible, and easy to extend.

## Repository Structure

```text
.
├── app.py                       # Streamlit dashboard UI
├── ingestion_db.py              # Raw CSV -> SQLite ingestion pipeline
├── get_vendor_summary.py        # SQL aggregation + pandas metric engineering
├── Exploratory_Data_Analysis.ipynb
├── Vendor_Performance_Analysis.ipynb
├── README.md
├── requirements.txt
├── data/                        # Source CSV extracts
└── logs/                        # Pipeline logs
```

## Data Flow

1. Place source CSV files in the `data/` directory.
2. Run `ingestion_db.py` to create/update raw SQLite tables.
3. Run `get_vendor_summary.py` to build the curated `vendor_sales_summary` table.
4. Query the summary table from notebooks, BI tools, or ad-hoc SQL.

## Expected Raw Tables

The transformation script currently expects these SQLite tables to exist:

- `purchases`
- `purchase_prices`
- `sales`
- `vendor_invoice`

These are created automatically if matching CSV files exist in `data/` and are named in a way that normalizes to the same table names.

## Engineering Notes

### Why SQLite

SQLite is a good fit here because the project is local, easy to version around, and simple for analysts to inspect without standing up infrastructure.

### Why Chunked Ingestion

`pandas.read_csv(..., chunksize=...)` lets the ingestion script handle large extracts without reading the full file into memory. This is the main optimization used in the raw-data load stage.

### Why SQL for Aggregation

The vendor summary logic performs grouped aggregation in SQLite first, which is usually more efficient than loading all raw rows into pandas and joining there.

### Reliability Improvements Included

- Stable table naming from filenames.
- Structured logging for both ingestion and transformation scripts.
- Division-by-zero protection for ratio metrics.
- Clean script entry points via `main()`.
- Replacement of the final summary table on each run for consistent outputs.

## How to Run

### 1. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 2. Ingest raw CSV files

```bash
python ingestion_db.py
```

### 3. Build the vendor summary

```bash
python get_vendor_summary.py
```

Note: ingestion is idempotent per source file. Each file refresh replaces the matching SQLite table on the first chunk, then appends the remaining chunks.

## Output Tables

### Raw tables

Each CSV file becomes a SQLite table with a lowercase, underscore-separated name derived from its filename.

### Curated table

`vendor_sales_summary` includes the following analysis-ready fields:

- `VendorNumber`
- `VendorName`
- `Brand`
- `PurchasePrice`
- `ActualPrice`
- `TotalPurchaseQuantity`
- `TotalPurchaseDollars`
- `TotalSalesQuantity`
- `TotalSalesDollars`
- `TotalSalesPrice`
- `TotalExciseTax`
- `FreightCost`
- `GrossProfit`
- `ProfitMargin`
- `StockTurnover`
- `SalesToPurchaseRatio`

## Final Documentation for Engineers and Data Analysts

### For Engineers

#### Pipeline architecture

- **Ingestion layer:** `ingestion_db.py` scans `data/`, streams CSVs in chunks, and appends them into SQLite.
- **Transformation layer:** `get_vendor_summary.py` uses SQL common table expressions (CTEs) to aggregate purchases, sales, and freight before pandas adds analytical KPIs.
- **Consumption layer:** the final `vendor_sales_summary` table can be consumed from Python, notebooks, or external BI tools that support SQLite.

#### Operational guidance

- Keep source file names stable so downstream table names remain predictable.
- If a new raw dataset is added, confirm the normalized CSV filename matches the SQL table name expected by transformation logic.
- Review `logs/ingestion_db.log` and `logs/get_vendor_summary.log` when debugging failed loads.
- If the business metric definitions change, update them centrally in `clean_data()` rather than in multiple notebooks.

#### Extension ideas

- Add unit tests around `build_table_name()` and `clean_data()`.
- Add data validation checks for required columns before ingestion.
- Parameterize the database path and data directory using CLI arguments.
- Add indexing on frequently joined SQLite columns if dataset size grows materially.

### For Data Analysts

#### What the summary table answers

The curated table helps answer questions such as:

- Which vendors and brands generate the highest purchase spend?
- Which vendors are producing the strongest gross profit and margin?
- Are there brands with weak stock turnover or low sales-to-purchase efficiency?
- How much freight cost is associated with each vendor?

#### KPI definitions

- **GrossProfit** = `TotalSalesDollars - TotalPurchaseDollars`
- **ProfitMargin** = `GrossProfit / TotalSalesDollars * 100`
- **StockTurnover** = `TotalSalesQuantity / TotalPurchaseQuantity`
- **SalesToPurchaseRatio** = `TotalSalesDollars / TotalPurchaseDollars`

#### Recommended analysis workflow

1. Run ingestion after refreshing raw CSV extracts.
2. Rebuild `vendor_sales_summary`.
3. Validate row counts and null behavior.
4. Use the summary table for segmentation by vendor, brand, profitability, and sales efficiency.
5. Reserve raw tables for audit and root-cause investigation.

#### Example SQL

```sql
SELECT
    VendorName,
    SUM(TotalSalesDollars) AS sales,
    SUM(GrossProfit) AS gross_profit,
    AVG(ProfitMargin) AS avg_profit_margin
FROM vendor_sales_summary
GROUP BY VendorName
ORDER BY gross_profit DESC;
```

## Troubleshooting

- **No CSV files found:** confirm files exist in `data/` and have a `.csv` extension.
- **Missing table errors:** confirm the required raw CSVs were ingested and normalized to the expected table names.
- **Unexpected zeros in ratios:** this usually means the denominator was zero and the pipeline intentionally protected against division errors.

## Next Best Improvements

- Add automated tests.
- Add schema validation.
- Add a reproducible sample dataset.
- Add automated data quality checks for the curated summary table.

## Dashboard App

A minimalist Streamlit dashboard is now included for visualizing the curated `vendor_sales_summary` table.

### Dashboard features

- Clean KPI cards for sales, purchase spend, gross profit, and average margin.
- Sidebar filters for vendor, brand, and minimum margin.
- Overview visuals for top vendors, brand mix, profitability, and gross-profit leaders.
- A vendor explorer tab for drilling into one vendor's brand performance.
- A detailed data table for audit-friendly inspection.

### Run the dashboard

```bash
pip install -r requirements.txt
python -m streamlit run app.py
```

The app automatically reads from `inventory.db` and can refresh the summary table directly from the sidebar when the raw tables already exist.
