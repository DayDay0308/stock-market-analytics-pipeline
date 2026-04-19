import os
from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig, SchemaField
import pandas as pd
import io

PROJECT_ID = "zoomcamp-project-493610"
BUCKET_NAME = "zoomcamp-project-493610-stock-data-lake"
DATASET_ID = "stock_market_data"
TABLE_ID = "raw_stocks"
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "../credentials.json")

def main():
    print("=" * 60)
    print("  Loading GCS Data into BigQuery")
    print("=" * 60)

    bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_PATH)
    gcs_client = storage.Client.from_service_account_json(CREDENTIALS_PATH)

    bucket = gcs_client.bucket(BUCKET_NAME)
    blobs = [b for b in bucket.list_blobs(prefix="raw/stocks/") if b.name.endswith(".csv")]
    print(f"\nFound {len(blobs)} CSV files in GCS")

    all_dfs = []
    for blob in blobs:
        try:
            content = blob.download_as_text()
            df = pd.read_csv(io.StringIO(content))
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            print(f"Error reading {blob.name}: {e}")

    print(f"Combining all data...")
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df["date"] = pd.to_datetime(combined_df["date"]).dt.date
    combined_df = combined_df.dropna(subset=["close"])
    combined_df = combined_df.drop_duplicates(subset=["date", "ticker"])
    print(f"Total rows to load: {len(combined_df):,}")

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    schema = [
        SchemaField("date", "DATE", mode="NULLABLE"),
        SchemaField("ticker", "STRING", mode="NULLABLE"),
        SchemaField("exchange", "STRING", mode="NULLABLE"),
        SchemaField("open", "FLOAT64", mode="NULLABLE"),
        SchemaField("high", "FLOAT64", mode="NULLABLE"),
        SchemaField("low", "FLOAT64", mode="NULLABLE"),
        SchemaField("close", "FLOAT64", mode="NULLABLE"),
        SchemaField("volume", "FLOAT64", mode="NULLABLE"),
    ]

    job_config = LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )

    print(f"Loading to BigQuery...")
    job = bq_client.load_table_from_dataframe(
        combined_df, table_ref, job_config=job_config
    )
    job.result()

    table = bq_client.get_table(table_ref)
    print("\n" + "=" * 60)
    print(f"  Load Complete!")
    print(f"  Total rows in BigQuery: {table.num_rows:,}")
    print("=" * 60)

if __name__ == "__main__":
    main()