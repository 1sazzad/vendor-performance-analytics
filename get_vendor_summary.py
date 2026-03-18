import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db


logging.basicConfig(
    filename='logs/get_vendor_summary.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

def create_vendor_summary(conn):
    '''This function will merge the different tables to get the overall vendor summary and adding new columns in the resulting data '''
    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            AVG(p.PurchasePrice) AS PurchasePrice,
            AVG(pp.Price) AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
            AND p.VendorNumber = pp.VendorNumber
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand
    ),

    SalesSummary AS (
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

    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber

    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary

def clean_data(df):
    '''This function will clean the data by filling the missing values with 0 and removing any duplicates'''
    df.fillna(0, inplace=True)
    #df.drop_duplicates(inplace=True)

    # remove space from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    #df['Description'] = df['Description'].str.strip()

    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    
    return df

if __name__ == "__main__":
    # creating database connection 
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating vendor summary table....')
    symmary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data......')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data into database....')
    ingest_db(clean_df, 'Vendor sales summary', conn)
    logging.info('Data ingested successfully into database....')