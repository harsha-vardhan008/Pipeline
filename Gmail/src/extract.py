import pandas as pd
import base64
import os
import io
import configparser
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from email import message_from_bytes
from email.utils import parseaddr

# Load config file
config = configparser.ConfigParser()
config.read("config.config")

# Read paths from config
token_path = config["gmail"]["token_path"]

# Define the required Gmail scope
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

# Load credentials from token.json
creds = Credentials.from_authorized_user_file(token_path, SCOPES)

# Build Gmail service object
service = build('gmail', 'v1', credentials=creds)

# Test: List unread emails
def test_fetch_emails():
    results = service.users().messages().list(userId='me', labelIds=['INBOX'], q="is:unread").execute()
    messages = results.get('messages', [])
    raw_data = []
    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id']).execute()
        headers = {h['name']: h['value'] for h in msg_data['payload']['headers']}

        name,email = parseaddr(headers.get('From', ''))
        sender = name if name else email.split('@')[0]
        cc = headers.get('Cc', '')
        subject = headers.get('Subject', '')

        parts = msg_data['payload'].get('parts', [])
        body = ""
        attachments = []

        for part in parts:
            mime_type = part.get("mimeType")
            filename = part.get("filename")
            if mime_type == "text/plain" and "data" in part['body']:
                body = base64.urlsafe_b64decode(part['body']['data']).decode("utf-8")
            elif filename:
                att_id = part['body']['attachmentId']
                attachment = service.users().messages().attachments().get(userId='me', messageId=msg['id'], id=att_id).execute()
                file_data = base64.urlsafe_b64decode(attachment['data'])
                attachments.append({"filename": filename, "file_data": file_data})
        service.users().messages().modify(
        userId='me',
        id=msg['id'],
        body={'removeLabelIds': ['UNREAD']}
        ).execute()


        raw_data.append({
            "sender": sender,
            "cc": cc,
            "subject": subject,
            "body": body,
            "attachments": attachments
        })

    return raw_data