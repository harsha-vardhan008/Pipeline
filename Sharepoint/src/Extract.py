from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
import pandas as pd
import configparser
def load_config():
    config = configparser.ConfigParser()
    config.read(r"C:\Users\Harshavardhan\Documents\python_tutorials\Sharepoint\config.config")
    return {
        "site_url": config.get("sharepoint", "site_url"),
        "username": config.get("sharepoint", "username"),
        "password": config.get("sharepoint", "password")
    }

def sharepoint_conn(site_url, username, password):
    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(username, password):
        print(" SharePoint Connection Established Successfully")
        return ClientContext(site_url, ctx_auth)
    else:
        raise Exception(" Authentication failed!")

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

    df = pd.DataFrame(file_data)
    return df

