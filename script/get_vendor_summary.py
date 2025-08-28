import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db  # Make sure this module is correctly named

# Setup logging
logging.basicConfig(
    filename="logs/vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Step 1: Connect to the database
conn = sqlite3.connect('inventory.db')
logging.info("Connected to database.")

# Step 2: Run SQL to get vendor summary
query = """
WITH FreightSummary AS (
    SELECT VendorNumber, SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
),
PurchaseSummary AS (
    SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price AS ActualPrice,
        pp.Volume,
        SUM(p.Quantity) AS TotalPurchaseQuantity,
        SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
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
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC
"""

vendor_df = pd.read_sql_query(query, conn)
logging.info("Vendor summary query executed.")

# Step 3: Clean and enrich the data
vendor_df['Volume'] = vendor_df['Volume'].astype(float)
vendor_df.fillna(0, inplace=True)
vendor_df['VendorName'] = vendor_df['VendorName'].str.strip()
vendor_df['Description'] = vendor_df['Description'].str.strip()

vendor_df['GrossProfit'] = vendor_df['TotalSalesDollars'] - vendor_df['TotalPurchaseDollars']
vendor_df['ProfitMargin'] = (vendor_df['GrossProfit'] / vendor_df['TotalSalesDollars']) * 100
vendor_df['StockTurnover'] = vendor_df['TotalSalesQuantity'] / vendor_df['TotalPurchaseQuantity']
vendor_df['SalesToPurchaseRatio'] = vendor_df['TotalSalesDollars'] / vendor_df['TotalPurchaseDollars']

logging.info("Data cleaned and enriched.")

# Step 4: Ingest into database
ingest_db(vendor_df, 'vendor_sales_summary', conn)
logging.info("Data ingested into vendor_sales_summary table.")
