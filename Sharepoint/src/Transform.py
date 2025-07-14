from office365.runtime.auth.authentication_context import AuthenticationContext
import pandas as pd
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

