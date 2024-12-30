import psycopg2
from psycopg2.extras import execute_values

def load_data(data, table_name, db_config):
    """
    Load data into PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Prepare the insert query
        query = f"""
        INSERT INTO {table_name} (date, open, high, low, close, adj_close, volume)
        VALUES %s
        ON CONFLICT DO NOTHING;
        """

        # Ensure data is a list of tuples
        values = [
            (
                row[0],  # Date
                row[1],  # Open
                row[2],  # High
                row[3],  # Low
                row[4],  # Close
                row[5],  # Adj Close
                row[6]   # Volume
            )
            for row in data
        ]

        # Execute batch insert
        execute_values(cursor, query, values)

        conn.commit()
        print(f"Data successfully loaded into {table_name}.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()

