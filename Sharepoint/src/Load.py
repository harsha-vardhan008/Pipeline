import configparser
import pandas as pd
from sqlalchemy import create_engine
import urllib

def load_to_sql(df: pd.DataFrame, table_name: str):
    try:
        # Load SQL Server config
        config = configparser.ConfigParser()
        config.read("config.config")
        driver = config["ssms"]["driver"]
        server = config["ssms"]["server"]
        database = config["ssms"]["database"]
        trusted_connection = config["ssms"]["trusted_connection"]

        # Connection string
        conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection={trusted_connection};"
        encoded_conn_str = urllib.parse.quote_plus(conn_str)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}", fast_executemany=True)

        # Upload DataFrame to SQL Server
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"Data loaded into SQL Server table: '{table_name}'")

    except Exception as e:
        print(f" Failed to load data into '{table_name}':", e)
