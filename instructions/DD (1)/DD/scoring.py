import json
import pickle

import pandas as pd
import yfinance as yf
import psycopg2
from pyscopg2.extras import execute_values



def load_postgresql(data,table_name,db_config):
    """
    :return: connection success
            conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        #upload data 
        execute_values(cursor,query,values)

        conn.commit()
    """



def convert_data(raw_data):
    """

    :return: new dataframe with all columns.
    """
    with open('model/column_list.txt', 'r') as file:
        columns = [column.strip() for column in file.readlines()]
    empty_df = pd.DataFrame(columns=columns)
    return df.reindex(columns=empty_df.columns)


def read_postgresql(query,db_config):
    """
    This function read data from postgresql table. you can give query parameter which benefits to query filtering
    data. So you should convert data to pandas dataframe. 
    :param db_config: Database and host-port pass\ user parameters
    :param query: fetch data using this query
    :return: Pandas Dataframe
    """
    cur=conn.cursor()
    cur.execute("your query")
    rows = cur.fetchall()

    cur.close()
    conn.close()
    #Your Code Here



def get_scores(df):
    """
    This function will generates your scores. You will read your model from pickle file and them apply it onto the
    dataframe. It will return new dataframe with score column.
    :param df: Input data for model as dataframe
    :return: Output data with scores as dataframe
    """

    #Your Code Here


if __name__ == '__main__':
    """
    This your main function and flow will be here. 
    1. Read data from postgresql. These data should be loaded previously with ETL.
    6. Find Scores
    7. Write them to PostgreSQL
    """
    # Your Code Here
