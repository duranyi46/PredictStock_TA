from pyspark.sql import SparkSession
from extract import extract_data
from load import load_data
from transformation import transform_data

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Yahoo Finance to PostgreSQL") \
        .getOrCreate()

    # Define the stock index and additional tickers
    index_list = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'META', 'ADBE', 'TSLA', 'FFIE', 'ASTI', 'ALLR']

    # Step 1: Extract data
    print("Extracting data...")
    raw_data = extract_data(index_list)
    if not raw_data:
        print("No data extracted. Exiting.")
        return

    # Step 2: Transform data
    print("Transforming data...")
    transformed_data = transform_data(raw_data)
    if not transformed_data:
        print("No data transformed. Exiting.")
        return

    # Define database configuration
    db_config = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "ykd46sqldefne",  # Replace with your actual password
        "dbname": "Finance"
    }
    
    # Table name for loading data
    table_name = "historical_stock_data"

    # Step 3: Load data
    print("Loading data into PostgreSQL...")
    load_data(transformed_data, table_name, db_config)
    print("ETL pipeline completed successfully.")

    # Stop the Spark session
    spark.stop()

if __name__ == '__main__':
    main()
