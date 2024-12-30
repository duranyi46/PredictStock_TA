from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, LongType

from pyspark.sql.functions import to_date

def transform_data(raw_data, spark):
    """
    Transform raw data into a Spark DataFrame.
    """

    # Define schema for the DataFrame
    schema = StructType([
        StructField("Date", DateType(), True),
        StructField("Open", FloatType(), True),
        StructField("High", FloatType(), True),
        StructField("Low", FloatType(), True),
        StructField("Close", FloatType(), True),
        StructField("Adj Close", FloatType(), True),
        StructField("Volume", LongType(), True)  # Changed to LongType
    ])

    transformed_data = []

    for ticker, data in raw_data.items():
        try:
            print(f"Transforming data for {ticker}...")

            # Reset index to convert 'Date' from index to column
            data = data.reset_index()

            # Debugging: Print the columns of the DataFrame
            print(f"Columns in data for {ticker}: {data.columns.tolist()}")

            # Check if 'Date' column exists
            if 'Date' not in data.columns:
                print(f"Error: 'Date' column not found for {ticker}.")
                continue

            # Convert the 'Date' column to a date format (without time)
            data['Date'] = data['Date'].dt.date  # Convert to date

            # Convert Pandas DataFrame to Spark DataFrame
            spark_df = spark.createDataFrame(data, schema=schema)

            # Collect data back to a list of rows
            transformed_data.extend(spark_df.collect())

            print(f"Data for {ticker} transformed successfully.")
        except Exception as e:
            print(f"Error transforming data for {ticker}: {e}")

    spark.stop()
    return transformed_data