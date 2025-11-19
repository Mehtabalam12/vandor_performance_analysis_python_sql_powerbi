import sqlite
import pandas as pd 
import logging
from ingestion_db import ingest_db

logging.basicConfig( 
    filename = "logs/get_vandor_summary.logs",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s- %(message)s",
    filemode = "a"
)

def create_vendor_summary (conn):
    ''''this function will merge the different tables to get the overall summary and adding new columns in the resulting data''''
     vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS ( 
     select 
        VendorNumber, 
        SUM(Freight) as FreightCost
     from vendor_invoice 
     Group By VendorNumber 
),

PurchaseSummary AS (
    select 
      p.VendorNumber, p.VendorName , p.Brand , p.PurchasePrice, pp.Volume, p.Description,
      pp.Price as ActualPrice, 
      SUM(p.Quantity) as TotalPurchaseQuantity , 
      SUM(p.Dollars) as TotalPurchaseDollars 
FROM purchases p JOIN purchase_prices pp on p.brand= pp.brand 
where p.PurchasePrice > 0 
GROUP BY p.VendorNumber , p.VendorName , p.Brand 

),

SalesSummary AS (
    select 
    VendorNo, 
    Brand, 
    SUM(SalesDollars) as TotalSalesDollars , 
    SUM(SalesPrice) as TotalSalesPrice, 
    SUM(SalesQuantity) as TotalSalesQuantiy,
    SUM(ExciseTax) as TotalExciseTax FROM sales 
GROUP BY VendorNo , Brand
)

select 
   ps.VendorNumber, 
   ps.VendorName ,
   ps.Brand ,
   ps.Description , 
   ps.PurchasePrice,
   ps.ActualPrice,
   ps.Volume, 
   ps.TotalPurchaseQuantity , 
   ps.TotalPurchaseDollars , 
   ss.TotalSalesDollars , 
   ss.TotalSalesPrice, 
   ss.TotalSalesQuantiy,
   ss.TotalExciseTax,
   fs.FreightCost 
FROM PurchaseSummary ps 
LEFT JOIN SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs 
    ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC""", conn)

return vendor_sales_summary 


def clean data(df) :
''''this function will clean the data ''''
    #changing datatype to float 
    df['Volume'] = df['Volume'].astype('float64')

    #filling missing values with zero 
    df.fillna(0 , inplace = True) 

    #removing spaces from categorical column
    df['VendorName'] = df['VendorName'].str.strip()
    df['description'] = df['description'].str.strip()

    #creating new columns for better analysis
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']  
    vendor_sales_summary['ProfitMargin'] =( vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollar']) *100 
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantiy'] / vendor_sales_summary['TotalPurchaseQuantity'] 
    vendor_sales_summary['SalestoPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars'] 


   return df 


if __name__ == '__main__':
    #creating database connection 
    conn = sqlite3.connect('my_database.db')

    logging.info ("Create vendor summary table.....")
    summary_df = create_vendor_summary(conn)
    logging.info (summary_df.head())

    logging.info ("Cleaning data.....")
    clean_df =  clean_data(summary_df)
    logging.info (clean_df.head())

    logging.info ("Ingesting data.....")
    ingest_db(clean_df,'vendor_sales_summary', conn, 'replace')
    logging.info('Completed')