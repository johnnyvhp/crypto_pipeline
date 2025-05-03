import os
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv, find_dotenv

def main():
    # Load environment variables from the nearest .env file
    load_dotenv(find_dotenv())

    # Determine the path to the output directory reliably
    SCRIPT_DIR = Path(__file__).resolve().parent
    PARQUET_DIR = SCRIPT_DIR.parent / "output" / "crypto_data"
    print("🔍 Searching for Parquet files in:", PARQUET_DIR.resolve())

    GCP_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    print("🔑 GOOGLE_APPLICATION_CREDENTIALS:", GCP_CREDENTIALS)
    if not GCP_CREDENTIALS:
        raise EnvironmentError("❌ GOOGLE_APPLICATION_CREDENTIALS is not set. Check your .env or environment variables.")

    BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
    BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
    BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

    # Load the service account credentials manually
    credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS)

    # Initialize BigQuery client using the credentials
    client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)

    # Read all parquet files from all subdirectories
    all_parquet_files = list(PARQUET_DIR.rglob("*.parquet"))

    if not all_parquet_files:
        print("⚠️ No parquet files found in", PARQUET_DIR)
        return

    print("📂 Found the following Parquet files:")
    for file in all_parquet_files:
        print(" -", file)

    dfs = [pd.read_parquet(file) for file in all_parquet_files]
    df = pd.concat(dfs, ignore_index=True)

    # Remove duplicates if necessary
    df = df.drop_duplicates()

    print(f"✅ Loaded {len(df)} rows from {len(all_parquet_files)} parquet files.")

    # Define the table ID
    table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

    # Upload to BigQuery
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        ),
    )

    job.result()  # Waits for the job to complete

    print(f"✅ Successfully uploaded {len(df)} rows to BigQuery table {table_id}.")

if __name__ == "__main__":
    main()