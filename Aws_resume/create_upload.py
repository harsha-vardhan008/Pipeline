import boto3
import configparser
import os

config=configparser.ConfigParser()
config.read(r'C:\Users\Harshavardhan\Documents\python_tutorials\Aws_resume\config.config')

aws_access_key_id = config['aws']['aws_access_key_id']
aws_secret_access_key = config['aws']['aws_secret_access_key']
region_name = config['aws']['region_name']

#creating S3 client

s3=boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

response = s3.list_buckets()
for bucket in response['Buckets']:
    print(bucket['Name'])

# creating a new bucket
bucket_name= 'resume-to-txt'

try:
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint' : region_name}

    )
    print(f"bucket '{bucket_name}' created sucessfully")
except s3.exceptions.BucketAlreadyOwnedByYou:
    print(f"Bucket '{bucket_name }' already existss and is owened  by you ")
except Exception as e:
    print(f" Error :{e}")


bucket_n='resume-to-txt'
local_folder=r'C:\Users\Harshavardhan\OneDrive - Kasmo\Desktop\Resumes'

for root, dirs, files in os.walk(local_folder):
    for file in files:
        local_path=os.path.join(root,file)
        s3_key=os.path.relpath(local_path,local_folder).replace('\\','/')
        try:
            s3.upload_file(local_path,bucket_n,s3_key)
            print(f"uploaded:{s3_key}")
        except Exception as e:
            print(f"failed to upload")