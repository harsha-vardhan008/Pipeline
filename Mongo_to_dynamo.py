import pymongo as pm
import boto3
import configparser
import pandas as pd


config = configparser.ConfigParser()
config.read(r'C:\Users\Harshavardhan\Documents\python_tutorials\Dynamo\config.config') 
url=config['mongo']['url']
client = pm.MongoClient(url)
db = client["First"]
 
collection1 = db["Project"]
collection2 = db["Unstructured"]
 
cursor = collection1.find().batch_size(1)
cursor_2 = collection2.find()
 

 
aws_access_key_id = config['aws']['aws_access_key_id']
aws_secret_access_key = config['aws']['aws_secret_access_key']
region_name = config['aws']['region_name']
 
dynamodb = boto3.resource(
    'dynamodb',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)
 
try:
    table1 = dynamodb.create_table(
        TableName='project',
        KeySchema=[
            {
                'AttributeName': 'project_id',
                'KeyType': 'HASH'  
            },
            {
                'AttributeName': 'project_name',
                'KeyType': 'RANGE'  
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'project_id',
                'AttributeType': 'S'  
            },
            {
                'AttributeName': 'project_name',
                'AttributeType': 'S'  
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
 
    table2 = dynamodb.create_table(
        TableName='project_2',
        KeySchema=[
            {
                'AttributeName': 'project_id',
                'KeyType': 'HASH'  
            },
            {
                'AttributeName': 'project_name',
                'KeyType': 'RANGE'  
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'project_id',
                'AttributeType': 'S'  
            },
            {
                'AttributeName': 'project_name',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    print("table already exist pushing data to the same...")
 
table1 = dynamodb.Table('project')
table2 = dynamodb.Table('project_2')
 
with table1.batch_writer() as batch:
    for item in cursor:
        if '_id' in item:
            item['_id'] = str(item['_id'])
 
        batch.put_item(Item=item)
 
with table2.batch_writer() as batch:
    for item in cursor_2:
        if '_id' in item:
            item['_id'] = str(item['_id'])
           
        batch.put_item(Item=item)