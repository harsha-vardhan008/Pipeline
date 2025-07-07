import configparser
import urllib
from sqlalchemy import create_engine, text

def connect(autocommit=False):
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\FastAPI\config.config")
    
    driver = config['ssms']['driver']
    server = config['ssms']['server']
    database = config['ssms']['database']
    trusted_conn = config['ssms']['trusted_connection']

    params = urllib.parse.quote_plus(
        f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection={trusted_conn}"
    )

    # Set autocommit at engine level if needed
    if autocommit:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", isolation_level="AUTOCOMMIT")
    else:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    return engine

def create_db(db_name: str):
    #  Use autocommit-enabled engine
    engine = connect(autocommit=True)

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT name FROM sys.databases WHERE name = :db_name"),
                {"db_name": db_name}
            ).fetchone()

            if result:
                print(f" Database '{db_name}' already exists.")
            else:
                conn.execute(text(f"CREATE DATABASE [{db_name}]"))
                print(f" Database '{db_name}' created successfully.")

    except Exception as e:
        print(' Database not created:', e)

# create_db("Fast_API")


def create_table(table_name,db_name,autocommit=True):
        config = configparser.ConfigParser()
        config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\FastAPI\config.config")
        
        driver = config['ssms']['driver']
        server = config['ssms']['server']
        database =db_name
        trusted_conn = config['ssms']['trusted_connection']

        params = urllib.parse.quote_plus(
            f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection={trusted_conn}"
        )

        # Set autocommit at engine level if needed
        if autocommit:
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", isolation_level="AUTOCOMMIT")
        else:
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        
        create_stmt = f"""
        IF OBJECT_ID(N'{table_name}', 'U') IS NULL
        BEGIN
            CREATE TABLE {table_name} (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    title NVARCHAR(100),
                    description NVARCHAR(100),
                    completed BIT DEFAULT 0
                )
        END
            """
        try:
            with engine.connect() as conn:
                conn.execute(text(create_stmt))
                print(f"Table '{table_name}' created in '{db_name}'.")
        except Exception as e:
            print(f"Failed to create table '{table_name}': {e}")

# create_table('todo','Fast_API')


