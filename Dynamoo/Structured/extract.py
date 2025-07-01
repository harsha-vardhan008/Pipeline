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
    table = dynamodb.Table('project')

    # Get all items
    response = table.scan()
    items = response.get('Items', [])

    # Convert to DataFrame (optional)
    df = pd.DataFrame(items)
    return df
