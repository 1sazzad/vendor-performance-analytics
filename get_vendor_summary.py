"""Build an analysis-ready vendor summary table from the raw SQLite tables."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from ingestion_db import LOG_DIR


DATABASE_PATH = Path("inventory.db")
SUMMARY_TABLE_NAME = "vendor_sales_summary"


# Why this setup exists:
# - This transformation step deserves its own log stream because SQL joins and
#   metric calculations are usually where analytics pipelines fail.
# - Reusing the shared logs directory keeps operations organized.
logging.basicConfig(
    filename=LOG_DIR / "get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)
LOGGER = logging.getLogger(__name__)


VENDOR_SUMMARY_QUERY = """
WITH freight_summary AS (
    SELECT
        VendorNumber,
        SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
),
purchase_summary AS (
    SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        AVG(p.PurchasePrice) AS PurchasePrice,
        AVG(pp.Price) AS ActualPrice,
        SUM(p.Quantity) AS TotalPurchaseQuantity,
        SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases AS p
    JOIN purchase_prices AS pp
        ON p.Brand = pp.Brand
       AND p.VendorNumber = pp.VendorNumber
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand
),
sales_summary AS (
    SELECT
        VendorNo,
        Brand,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
)
SELECT
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    COALESCE(ss.TotalSalesQuantity, 0) AS TotalSalesQuantity,
    COALESCE(ss.TotalSalesDollars, 0) AS TotalSalesDollars,
    COALESCE(ss.TotalSalesPrice, 0) AS TotalSalesPrice,
    COALESCE(ss.TotalExciseTax, 0) AS TotalExciseTax,
    COALESCE(fs.FreightCost, 0) AS FreightCost
FROM purchase_summary AS ps
LEFT JOIN sales_summary AS ss
    ON ps.VendorNumber = ss.VendorNo
   AND ps.Brand = ss.Brand
LEFT JOIN freight_summary AS fs
    ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC
"""



def create_vendor_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    """Create the core vendor-level summary DataFrame from raw tables.

    Why we do this:
    - Analysts need one denormalized table instead of repeatedly joining raw
      purchase, sales, and invoice tables.
    - Performing the aggregation in SQL is faster than pulling raw tables into
      pandas and joining there.

    How it works:
    - Run a CTE-based SQL query that pre-aggregates freight, purchases, and
      sales separately.
    - Join the aggregates into one vendor-brand-level summary DataFrame.
    """

    return pd.read_sql_query(VENDOR_SUMMARY_QUERY, conn)



def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize the summary data and calculate business metrics.

    Why we do this:
    - Downstream dashboards and notebooks are easier to maintain when null
      handling and metric definitions are centralized in one function.
    - Copying the DataFrame avoids mutating upstream caller state.

    How it works:
    - Fill missing numeric values with zero.
    - Normalize vendor names by trimming spaces.
    - Use vectorized ``numpy``-style pandas operations for derived metrics.
    - Guard every division so ratios remain stable when a denominator is zero.
    """

    cleaned_df = df.copy()
    cleaned_df = cleaned_df.fillna(0)
    cleaned_df["VendorName"] = cleaned_df["VendorName"].astype(str).str.strip()

    gross_profit = cleaned_df["TotalSalesDollars"] - cleaned_df["TotalPurchaseDollars"]
    cleaned_df["GrossProfit"] = gross_profit

    sales_dollars = cleaned_df["TotalSalesDollars"]
    purchase_quantity = cleaned_df["TotalPurchaseQuantity"]
    purchase_dollars = cleaned_df["TotalPurchaseDollars"]

    cleaned_df["ProfitMargin"] = gross_profit.div(sales_dollars.where(sales_dollars.ne(0))).mul(100).fillna(0)
    cleaned_df["StockTurnover"] = cleaned_df["TotalSalesQuantity"].div(
        purchase_quantity.where(purchase_quantity.ne(0))
    ).fillna(0)
    cleaned_df["SalesToPurchaseRatio"] = sales_dollars.div(
        purchase_dollars.where(purchase_dollars.ne(0))
    ).fillna(0)

    return cleaned_df



def save_vendor_summary(df: pd.DataFrame, database_path: Path = DATABASE_PATH) -> None:
    """Persist the analytics-ready summary DataFrame back into SQLite.

    Why we do this:
    - Storing the curated output as its own table makes it easy for notebooks,
      BI tools, and ad-hoc SQL users to consume a stable dataset.

    How it works:
    - Open a SQLite connection using the local project database.
    - Replace the old summary table so each pipeline run produces one canonical
      fresh snapshot.
    """

    with sqlite3.connect(database_path) as conn:
        df.to_sql(SUMMARY_TABLE_NAME, conn, if_exists="replace", index=False)



def run_vendor_summary_pipeline(database_path: Path = DATABASE_PATH) -> pd.DataFrame:
    """Execute the end-to-end transformation pipeline and return the result.

    Why we do this:
    - Encapsulating the workflow in one function gives engineers a reusable API
      for scripts, tests, and future orchestration tools.

    How it works:
    - Read the raw warehouse tables from SQLite.
    - Build the vendor summary.
    - Clean and enrich the result with analysis metrics.
    - Save the final dataset and return it for immediate inspection.
    """

    LOGGER.info("Creating vendor summary from database '%s'.", database_path)
    with sqlite3.connect(database_path) as conn:
        summary_df = create_vendor_summary(conn)

    LOGGER.info("Vendor summary created with %s rows.", len(summary_df))
    cleaned_df = clean_data(summary_df)
    LOGGER.info("Vendor summary cleaned and enriched with derived metrics.")
    save_vendor_summary(cleaned_df, database_path=database_path)
    LOGGER.info("Vendor summary saved to table '%s'.", SUMMARY_TABLE_NAME)
    return cleaned_df



def main() -> None:
    """Run the vendor summary pipeline as a script.

    Why we do this:
    - Keeping the execution path inside ``main`` avoids accidental pipeline runs
      when this module is imported elsewhere.

    How it works:
    - Call the orchestration function and print a small preview for fast manual
      verification.
    """

    summary_df = run_vendor_summary_pipeline()
    print(summary_df.head())


if __name__ == "__main__":
    main()
