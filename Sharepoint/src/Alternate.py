import os
import pandas as pd
import configparser
import json
import boto3
import traceback
from pymongo import MongoClient
from sqlalchemy import create_engine
import urllib
from pathlib import Path
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext

# ---------------- Load Config ----------------
def load_config():
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\Sharepoint\config.config")
    return {
        "site_url": config.get("sharepoint", "site_url"),
        "username": config.get("sharepoint", "username"),
        "password": config.get("sharepoint", "password"),
        "sql_driver": config.get("ssms", "driver"),
        "sql_server": config.get("ssms", "server"),
        "sql_database": config.get("ssms", "database"),
        "sql_trusted_connection": config.get("ssms", "trusted_connection"),
        "s3_bucket": config.get("aws", "bucket"),
        "aws_access_key": config.get("aws", "access_key"),
        "aws_secret_key": config.get("aws", "secret_key"),
        "aws_region_name":config.get("aws","region_name"),
        "mongo_url": config.get("mongo", "url"),
    }

# ---------------- SharePoint Connection ----------------
def sharepoint_conn(site_url, username, password):
    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        print(" SharePoint Connected")
        return ClientContext(site_url, ctx_auth)
    else:
        raise Exception("SharePoint Authentication failed!")

# ---------------- Fetch Files ----------------
def fetch_files_from_folder(ctx, folder_path):
    folder = ctx.web.get_folder_by_server_relative_url(folder_path)
    files = folder.files.get().execute_query()

    file_data = []
    for file in files:
        file_data.append({
            "FileName": file.properties["Name"],
            "ServerRelativeUrl": file.properties["ServerRelativeUrl"],
            "TimeLastModified": file.properties["TimeLastModified"]
        })
    return pd.DataFrame(file_data)

# ---------------- Download File ----------------
def download_file(ctx, server_relative_url, local_path):
    with open(local_path, "wb") as f:
        ctx.web.get_file_by_server_relative_url(server_relative_url).download(f).execute_query()
    print(f"Downloaded: {local_path}")

# ---------------- Load to SQL Server ----------------
def load_to_sql(df, config, table_name):
    conn_str = f"DRIVER={config['sql_driver']};SERVER={config['sql_server']};DATABASE={config['sql_database']};Trusted_Connection={config['sql_trusted_connection']};"
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(conn_str)}")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Loaded to SQL: {table_name}")

# ---------------- Load to MongoDB ----------------
def load_to_mongo(data, config, collection_name):
    client = MongoClient(config['mongo_url'])
    db = client.get_default_database()
    db[collection_name].insert_one(data)
    print(f" Loaded to MongoDB: {collection_name}")

# ---------------- Upload to S3 ----------------
def upload_to_s3(local_path, file_name, config):
    s3 = boto3.client('s3', aws_access_key_id=config['aws_access_key'], aws_secret_access_key=config['aws_secret_key'])
    s3.upload_file(local_path, config['s3_bucket'], file_name)
    print(f" Uploaded to S3: {file_name}")

# ---------------- Main ----------------
def main():
    try:
        config = load_config()
        ctx = sharepoint_conn(config['site_url'], config['username'], config['password'])

        folder_path = "/sites/kasmo-training/Shared Documents/Harsha Vardhan"
        df_files = fetch_files_from_folder(ctx, folder_path)

        os.makedirs("downloaded_files", exist_ok=True)

        for _, row in df_files.iterrows():
            file_path = Path(row["FileName"])
            server_url = row["ServerRelativeUrl"]
            local_path = Path("downloaded_files") / file_path.name

            download_file(ctx, server_url, str(local_path))

            if file_path.suffix in [".xlsx", ".csv"]:
                df = pd.read_excel(local_path) if file_path.suffix == ".xlsx" else pd.read_csv(local_path)
                load_to_sql(df, config, table_name=file_path.name)

            elif file_path.suffix == ".json":
                with open(local_path, "r") as f:
                    json_data = json.load(f)
                load_to_mongo(json_data, config, collection_name="SharePointJSON")

            elif file_path.suffix.lower() in [".pdf", ".png", ".jpg", ".jpeg"]:
                upload_to_s3(str(local_path), file_path.name, config)

            else:
                print(f" Skipped unsupported file type: {file_path.suffix}")

    except Exception as e:
        print(" Pipeline Error:", e)
        traceback.print_exc()
if __name__ == "__main__":
    main()
