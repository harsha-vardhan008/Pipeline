import os
import pandas as pd
from Extract import load_config, sharepoint_conn
from Load import load_to_sql
from Transform import fetch_files_from_folder
def download_file(ctx, server_relative_url, local_path):
    """Download a file from SharePoint to local"""
    with open(local_path, "wb") as f:
        ctx.web.get_file_by_server_relative_url(server_relative_url).download(f).execute_query()
    print(f" File downloaded: {local_path}")

if __name__ == "__main__":
    try: 
        #  Load config and connect to SharePoint
        config = load_config()
        ctx = sharepoint_conn(config["site_url"], config["username"], config["password"])

        #  Set SharePoint folder path and fetch files
        folder_path = "/sites/kasmo-training/Shared Documents/Harsha Vardhan"
        df_files = fetch_files_from_folder(ctx, folder_path)

        #  Filter for latest Excel file
        excel_files = df_files[df_files["FileName"].str.endswith(".xlsx")]
        if excel_files.empty:
            print(" No Excel files found in the folder.")
            exit()

        latest_file = excel_files.sort_values("TimeLastModified", ascending=False).iloc[0]
        server_relative_url = latest_file["ServerRelativeUrl"]
        file_name = latest_file["FileName"]
        local_path = os.path.join("downloaded_files", file_name)

        # Download file
        os.makedirs("downloaded_files", exist_ok=True)
        download_file(ctx, server_relative_url, local_path)

        # Read Excel into DataFrame
        df_excel = pd.read_excel(local_path)
        print(" Excel File Preview:")
        print(df_excel.head())

        # Load to SQL Server
        load_to_sql(df_excel, table_name="SharePointProjects")

    except Exception as e:
        print(" Error in pipeline:", e)
