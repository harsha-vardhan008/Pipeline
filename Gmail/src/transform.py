# import pandas as pd
# import configparser
# from email.utils import parseaddr

# def transform_email_data(raw_data):
#     # Load bucket name from config
#     config = configparser.ConfigParser()
#     config.read("config.config")
#     bucket = config["aws"]["bucket"]

#     email_records = []
#     attachment_records = []

#     for email in raw_data:
#         # Extract clean sender and receiver names
#         sender_name, sender_email = parseaddr(email["sender"])

#         # Fallback if names are missing
#         sender_name = sender_name if sender_name else sender_email.split("@")[0]
#         subject = email["subject"]

#         # Email metadata row
#         email_records.append({
#             "sender_name": sender_name,
#             "cc": email.get("cc", ""),
#             "subject": subject,
#             "body": email.get("body", "")
#         })

#         # Attachment records
#         for filename, _ in email.get("attachments", []):
#             s3_path = f"s3://{bucket}/{sender_email}/{filename}"
#             attachment_records.append({
#                 "email_subject": subject,
#                 "sender_name": sender_name,
#                 "attachment_filename": filename,
#                 "s3_url": s3_path
#             })

#     email_df = pd.DataFrame(email_records)
#     attachment_df = pd.DataFrame(attachment_records)
#     print(attachment_df)
#     return email_df, attachment_df

# ---------- transform.py ----------
import pandas as pd
import configparser
import boto3
from uuid import uuid4

def transform_email_data(raw_data):
    config = configparser.ConfigParser()
    config.read("config.config")
    aws_access_key_id = config["aws"]["aws_access_key_id"]
    aws_secret_access_key = config["aws"]["aws_secret_access_key"]
    region_name = config["aws"]["region_name"]
    bucket = config["aws"]["bucket"]

    s3 = boto3.client("s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    records = []

    for email in raw_data:
        attachment_urls = []
        for attachment in email["attachments"]:
            filename = attachment["filename"]
            content = attachment["file_data"]
            s3_key = f"{email['sender']}_attachments/{uuid4().hex}_{filename}"

            # Upload to S3
            s3.put_object(Bucket=bucket, Key=s3_key, Body=content)

            # Create public S3 URL or presigned (if needed)
            s3_url = f"https://{bucket}.s3.{region_name}.amazonaws.com/{s3_key}"
            attachment_urls.append(s3_url)

        record = {
            "sender_name": email["sender"],
            "cc": email["cc"],
            "subject": email["subject"],
            "body": email["body"],
            "attachment_1_url": attachment_urls[0] if len(attachment_urls) > 0 else None,
            "attachment_2_url": attachment_urls[1] if len(attachment_urls) > 1 else None
        }
        records.append(record)

    return pd.DataFrame(records)
