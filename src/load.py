import pyodbc
from sqlalchemy import create_engine
import pandas as pd
import configparser
import urllib

def load_to_mysql(df: pd.DataFrame, table_name: str):
    try:
        # Load MySQL config
        config = configparser.ConfigParser()
        config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\ETL Pipeline\src\config.config")

        if 'MySQL' not in config:
            raise KeyError("MySQL section not found in config file")

        # Read config (no re-initialization)
        host     = config['MySQL']['host']
        user     = config['MySQL']['user']
        password = config['MySQL']['password']
        database = config['MySQL']['database']
        driver   = config['MySQL']['driver']

        encoded_password = urllib.parse.quote_plus(password)

        connection_string = f"mysql+{driver}://{user}:{encoded_password}@{host}/{database}"
        engine = create_engine(connection_string)

        # Load to MySQL
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"Data inserted into MySQL table '{table_name}'.")
        return engine

    except Exception as e:
        print(f" Failed to insert into MySQL table '{table_name}':", e)
       

#Loading data to SSMS

def load_to_sqlserver(df: pd.DataFrame, table_name: str):
    # Load SQL Server config
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\ETL Pipeline\src\config.config")

    if 'ssms' not in config:
        raise KeyError("ssms section not found in config file")

    Driver = config['ssms']['Driver']
    Server = config['ssms']['Server']
    Database = config['ssms']['Database']
    trusted_conn = config['ssms']['Trusted_Connection']

    # URL encode server/driver if needed
    params = urllib.parse.quote_plus(
        f"DRIVER={Driver};SERVER={Server};DATABASE={Database};Trusted_Connection={trusted_conn}"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine
    # Load to SQL Server
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    print(f" Data inserted into SQL Server table '{table_name}'.")

def get_sqlserver_engine():
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\ETL Pipeline\src\config.config")

    Driver = config['ssms']['Driver']
    Server = config['ssms']['Server']
    Database = config['ssms']['Database']
    trusted_conn = config['ssms']['Trusted_Connection']

    params = urllib.parse.quote_plus(
        f"DRIVER={Driver};SERVER={Server};DATABASE={Database};Trusted_Connection={trusted_conn}"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine
