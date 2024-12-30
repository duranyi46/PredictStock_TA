import psycopg2
from pyscopg2.extras import execute_values

def load_data(data,table_name,db_config):
    """
    This function gets finance info dataframe. It loads data to PostgreSQL in separate collections
    but in the same database. You should be careful about the write mode and you can find the sample code in this url.


    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        #call transformation function(values variable) and write Sql query (query variable)

        #upload data 
        execute_values(cursor,query,values)

        conn.commit()
        print(f"Data succssfully loaded into {table_name}.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")

    finally:
        if conn:
            conn.close()    

  
    """
    #Your Code Here
