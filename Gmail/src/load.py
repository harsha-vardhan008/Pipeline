import configparser
import pandas as pd
from sqlalchemy import create_engine
import urllib
from extract import test_fetch_emails
from transform import transform_email_data

def load_to_sql(df: pd.DataFrame, table_name: str):
    config = configparser.ConfigParser()
    config.read("config.config")
    driver = config["ssms"]["driver"]
    server = config["ssms"]["server"]
    database = config["ssms"]["database"]
    trusted_connection = config["ssms"]["trusted_connection"]

    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection={trusted_connection};"
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print(f"Data loaded into '{table_name}'")

