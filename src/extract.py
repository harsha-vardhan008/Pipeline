import datetime
import pyodbc
import configparser
import pandas as pd
from sqlalchemy import create_engine,text
import urllib

def connect():
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\Task 24\config.config")

    print("Sections found:", config.sections())

    #connecting SQL SERVER
    Driver = config['ssms']['Driver']
    Server = config['ssms']['Server']
    Database = config['ssms']['Database']
    trusted_conn = config['ssms']['Trusted_Connection']

    conn = pyodbc.connect(
            f'Driver={Driver};'
            f'Server={Server};'
            f'Database={Database};'
            f'Trusted_Connection=yes;'

    )

    return conn
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
print(read_incremental_data())

# Connecting to MYSQL 
def mysql_read_data():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\Harshavardhan\Documents\python_tutorials\Task 24\config.config')
    
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

print(mysql_read_data())

def read_new_orders():

    query = 'select * from [dbo].[new_orders];'
    new_orders = pd.read_sql(query, connect())

    return new_orders
