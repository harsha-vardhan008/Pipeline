import os,json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

# This scope gives read-only access to Gmail
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

Credentials_path="C:/Users/Harshavardhan/Downloads/credentials.json"
Token_path="C:/Users/Harshavardhan/Downloads/token.json"

def generate_token():
    creds = None

    if os.path.exists('token.json'):
        with open(Token_path,"r") as token_file:
            creds = Credentials.from_authorized_user_file(json.load(token_file), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Replace 'credentials.json' with your file's path if needed
            flow = InstalledAppFlow.from_client_secrets_file(Credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the token for future use
        with open(Token_path, 'w') as token_file:
            token_file.write(creds.to_json())
        print("✅ token.json generated and saved.")
    return build("gmail","v1",credentials=creds)

generate_token()
