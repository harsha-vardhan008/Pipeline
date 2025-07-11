from extract import test_fetch_emails
from transform import transform_email_data
from load import load_to_sql



def run_etl():
    print(" Extracting unread emails...")
    raw_data = test_fetch_emails()

    if not raw_data:
        print("No unread emails found.")
        return

    print(" Transforming & Uploading attachments to S3...")
    transformed_df = transform_email_data(raw_data)

    print(" Loading data into SQL Server...")
    load_to_sql(transformed_df, "Email_Communications")

    print(" ETL process completed.")

if __name__ == "__main__":
    run_etl()
