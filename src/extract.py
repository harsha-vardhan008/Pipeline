import datetime
import pyodbc
import configparser
import pandas as pd
from sqlalchemy import create_engine,text
import urllib

def connect():
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\ETL Pipeline\src\config.config")

    print("Sections found:", config.sections())

    Driver = config['ssms']['Driver']
    Server = config['ssms']['Server']
    Database = config['ssms']['Database']
    trusted_conn = config['ssms']['Trusted_Connection']

    # NEW: Use SQLAlchemy-compatible engine
    params = urllib.parse.quote_plus(
        f"DRIVER={Driver};SERVER={Server};DATABASE={Database};Trusted_Connection={trusted_conn}"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine  #  SQLAlchemy engine
    print("Reading config from: C:\\Users\\Harshavardhan\\Documents\\python_tutorials\\ETL\\config.config")


# Connecting to SSMS
# Configuring the credentials
def read_data():

    conn=connect()
    # Loading the data from SQL Server

    orders_query='select * from sales.orders'
    oi_query='select * from sales.order_items'

    orders=pd.read_sql_query(orders_query,conn)
    order_items=pd.read_sql_query(oi_query,conn)

    # Merging the Tables
    
    data=orders.merge(order_items ,on='order_id',how='inner')

    return data
def read_incremental_data():

    conn=connect()
    query = 'select * from dbo.incremental'
    incremental = pd.read_sql_query(query, conn)

    today = datetime.date.today()
    incremental = incremental[incremental['shipped_date'] <= today]

    return incremental


# Connecting to MYSQL 
def mysql_read_data():
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\ETL Pipeline\src\config.config")
    
    host     = config['MySQL']['host']
    username = config['MySQL']['user']
    password = config['MySQL']['password']
    database = config['MySQL']['database']
    driver   = config['MySQL']['driver']
    
    encoded_password = urllib.parse.quote_plus(password)
    
    connection_string = f"mysql+{driver}://{username}:{encoded_password}@{host}/{database}"
    engine = create_engine(connection_string)
    
    # orders = pd.read_csv(r"C:\Users\Harshavardhan\Documents\python_tutorials\orders.csv")
    # orders.to_sql(name="orders", con=engine, if_exists="replace", index=False)
    
    # read_orders = pd.read_sql_table('orders', con=engine)
    # print(read_orders)
    

    orders='select * from orders'
    

    orders=pd.read_sql_query(orders,con=engine)
    return orders

# print(mysql_read_data())

def read_new_orders():

    query = 'select * from [dbo].[new_orders];'
    new_orders = pd.read_sql(query, connect())

    return new_orders

