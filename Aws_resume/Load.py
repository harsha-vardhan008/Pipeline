import configparser
from sqlalchemy import create_engine
import urllib

def load_to_sql(df):
    config = configparser.ConfigParser()
    config.read('config.config')

    params = urllib.parse.quote_plus(
        f"DRIVER={config['sqlserver']['driver']};"
        f"SERVER={config['sqlserver']['server']};"
        f"DATABASE={config['sqlserver']['database']};"
        f"Trusted_Connection={config['sqlserver']['trusted_connection']}"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    df.to_sql('resume_profiles', con=engine, if_exists='append', index=False)

