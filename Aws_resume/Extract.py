import boto3
import configparser

def extract_text_files():
    config = configparser.ConfigParser()
    config.read('config.config')

    s3 = boto3.client(
        's3',
        aws_access_key_id=config['aws']['aws_access_key_id'],
        aws_secret_access_key=config['aws']['aws_secret_access_key'],
        region_name=config['aws']['region_name']
    )

    bucket_name = 'cleaned-txt'
    data = []

    response = s3.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.txt'):
            file_obj = s3.get_object(Bucket=bucket_name, Key=key)
            content = file_obj['Body'].read().decode('utf-8')
            data.append({'filename': key, 'content': content})

    return data
