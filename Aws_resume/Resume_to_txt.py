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

# Key = filename/path inside the bucket

from pathlib import Path
 
def saved_parsed_data(parsed_data,folder_name):
    ouput_folder=Path(folder_name)
    ouput_folder.mkdir(exist_ok=True)
 
    for s3_key, text_content in parsed_data.items():
        pdf_filename=Path(s3_key).name
        text_format=pdf_filename.replace(".pdf",".txt")
        full_text_path=text_format
 
        with Path(full_text_path).open(mode="w",encoding="utf-8") as file:
            file.write(text_content)  

if __name__ == "__main__":
    process_pdfs()
    saved_parsed_data()

response = s3.list_objects_v2(Bucket='cleaned-txt')

for obj in response.get('Contents', []):
    print(obj['Key'])  



# import boto3
# import PyPDF2
# import io
# import configparser
# from pathlib import Path

# # Load AWS credentials from config
# config = configparser.ConfigParser()
# config.read(r'C:\Users\Harshavardhan\Documents\python_tutorials\Aws_resume\config.config')

# aws_access_key_id = config['aws']['aws_access_key_id']
# aws_secret_access_key = config['aws']['aws_secret_access_key']
# region_name = config['aws']['region_name']

# # Create S3 client
# s3 = boto3.client(
#     's3',
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key,
#     region_name=region_name
# )

# source_bucket = 'resume-to-txt'      # PDFs are here
# cleaned_bucket = 'cleaned-txt'       # Extracted text will go here
# archive_bucket = 'archive-txt'       # PDFs will be archived here

# # Extract text from PDF bytes
# def extract_pdf_text(pdf_bytes):
#     reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
#     text = ''.join([page.extract_text() or '' for page in reader.pages])
#     return text

# # Process PDFs in source bucket
# def process_pdfs():
#     objects = s3.list_objects_v2(Bucket=source_bucket)
#     if 'Contents' not in objects or len(objects['Contents']) == 0:
#         print("❌ No PDF files found in source bucket.")
#         return {}
#     parsed_data = {}

#     for obj in objects.get('Contents', []):
#         key = obj['Key']
#         if not key.lower().endswith('.pdf'):
#             continue

#         print(f"\n📄 Processing: {key}")

#         try:
#             # Download the PDF
#             file_obj = s3.get_object(Bucket=source_bucket, Key=key)
#             pdf_bytes = file_obj['Body'].read()

#             # Extract text
#             extracted_text = extract_pdf_text(pdf_bytes)
#             char_count = len(extracted_text)
#             print(f"   ➜ Extracted {char_count} characters")

#             # Store in parsed_data even if empty
#             parsed_data[key] = extracted_text

#             if extracted_text.strip():
#                 # Upload to cleaned bucket
#                 s3.put_object(
#                     Bucket=cleaned_bucket,
#                     Key=key.replace('.pdf', '.txt'),
#                     Body=extracted_text.encode('utf-8')
#                 )
#                 print(f"   ✅ Uploaded to {cleaned_bucket}")
#             else:
#                 print(f"   ⚠️ Skipped upload - No extractable text")

#             # Archive original
#             s3.copy_object(
#                 Bucket=archive_bucket,
#                 CopySource={'Bucket': source_bucket, 'Key': key},
#                 Key=key
#             )
#             s3.delete_object(Bucket=source_bucket, Key=key)
#             print(f"   📦 Moved to archive: {archive_bucket}")

#         except Exception as e:
#             print(f"   ❌ Failed to process {key}: {e}")

#     return parsed_data


# # Save parsed text to local .txt files
# def saved_parsed_data(parsed_data, folder_name="extracted_resume"):
#     output_folder = Path(folder_name)
#     output_folder.mkdir(exist_ok=True)

#     for s3_key, text_content in parsed_data.items():
#         pdf_filename = Path(s3_key).name
#         text_format = pdf_filename.replace(".pdf", ".txt")
#         full_text_path = output_folder / text_format

#         try:
#             with full_text_path.open(mode="w", encoding="utf-8") as file:
#                 file.write(text_content)
#             print(f"📁 Saved: {full_text_path}")
#         except Exception as e:
#             print(f"❌ Failed to save {full_text_path}: {e}")

# # Show cleaned bucket contents
# def show_cleaned_txt():
#     response = s3.list_objects_v2(Bucket=cleaned_bucket)
#     print("\n📝 Cleaned Text Files in S3:")
#     for obj in response.get('Contents', []):
#         print(" -", obj['Key'])

# # Entry point
# if __name__ == "__main__":
#     print("🔍 Extracting and parsing resumes...")
#     parsed_data = process_pdfs()
#     print(f"\n🧪 Total resumes parsed: {len(parsed_data)}")

#     print("\n💾 Saving locally...")
#     saved_parsed_data(parsed_data)

#     show_cleaned_txt()

