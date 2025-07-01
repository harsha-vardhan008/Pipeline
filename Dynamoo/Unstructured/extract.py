<<<<<<< HEAD
import pandas as pd
import boto3
import configparser

def extract():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\Harshavardhan\Documents\python_tutorials\Dynamo\config.config') 

    # Connect to DynamoDB
    
    aws_access_key_id = config['aws']['aws_access_key_id']
    aws_secret_access_key = config['aws']['aws_secret_access_key']
    region_name = config['aws']['region_name']
    
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Select table
    table = dynamodb.Table('project_2')

    # Get all items
    response = table.scan()
    items = response.get('Items', [])

    # Convert to DataFrame (optional)
    df = pd.DataFrame(items)
    return df
=======
import configparser
import pandas as pd 
from  pymongo import MongoClient

def extract():
    config=configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\MangoDB\config.config")
    url=config['mongo']['url']
    client=MongoClient(url)
    db= client['First']
    collection=db["Unstructured"]
    doc=list(collection.find())
    df=pd.DataFrame(doc)
    return df

extract()
>>>>>>> 703d8bdf800d01b810c45363edc031b117b13b01
