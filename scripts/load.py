import psycopg2
from psycopg2.extras import execute_values

def create_table(conn, table_name):
    """
    Create the table in PostgreSQL if it does not exist.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        date DATE NOT NULL,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        adj_close FLOAT,
        volume BIGINT,
        stock_name VARCHAR(9),
        PRIMARY KEY (date, stock_name)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created or already exists.")

def load_data(data, table_name, db_config):
    """
    Load data into PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**db_config)
        
        # Create the table if it does not exist
        create_table(conn, table_name)

        cursor = conn.cursor()

        # Prepare the insert query
        query = f"""
        INSERT INTO {table_name} (date, adj_close, close, high, low, open, volume, stock_name)
        VALUES %s
        ON CONFLICT DO NOTHING;
        """

        # Ensure data is a list of tuples
        values = [
            (
                row[0],  # Date
                row[1],  # adj_close
                row[2],  # close
                row[3],  # high
                row[4],  # low
                row[5],  # open 
                row[6],  # Volume
                row[7]   # Stock_Name
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

