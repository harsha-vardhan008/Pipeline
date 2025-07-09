import boto3
import PyPDF2
import io
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



source_bucket = 'resume-to-txt'       # Where PDFs are currently stored
cleaned_bucket = 'cleaned-txt'     # Where extracted text goes
archive_bucket = 'archive-txt'     # Where original PDFs are moved

def extract_pdf_text(pdf_bytes):
    reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
    text = ''.join([page.extract_text() or '' for page in reader.pages])
    return text

def process_pdfs():
    objects = s3.list_objects_v2(Bucket=source_bucket)

    for obj in objects.get('Contents', []):
        key = obj['Key']
        if not key.lower().endswith('.pdf'):
            continue

        print(f"Processing: {key}")

        # --- Get PDF content
        file_obj = s3.get_object(Bucket=source_bucket, Key=key)
        pdf_bytes = file_obj['Body'].read()

        # --- Extract text
        extracted_text = extract_pdf_text(pdf_bytes)

        if extracted_text.strip():
            # --- Upload extracted text to cleaned bucket
            s3.put_object(
                Bucket=cleaned_bucket,
                Key=key.replace('.pdf', '.txt'),
                Body=extracted_text.encode('utf-8')
            )
            print(f" Extracted text uploaded to {cleaned_bucket}")

        # --- Move original PDF to archive
        s3.copy_object(
            Bucket=archive_bucket,
            CopySource={'Bucket': source_bucket, 'Key': key},
            Key=key
        )
        s3.delete_object(Bucket=source_bucket, Key=key)
        print(f" Moved original PDF to {archive_bucket}")

if __name__ == "__main__":
    process_pdfs()

response = s3.list_objects_v2(Bucket='cleaned-txt')

for obj in response.get('Contents', []):
    print(obj['Key'])  # Key = filename/path inside the bucket
