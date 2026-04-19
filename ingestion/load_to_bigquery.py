import os
from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig, SchemaField
import pandas as pd
import io

# Configuration
PROJECT_ID = "zoomcamp-project-493610"
BUCKET_NAME = "zoomcamp-project-493610-stock-data-lake"
DATASET_ID = "stock_market_data"
TABLE_ID = "raw_stocks"
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "../credentials.json")

def create_table_if_not_exists(bq_client):
    """Create BigQuery table with partitioning and clustering"""
    dataset_ref = bq_client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)

    schema = [
        SchemaField("date", "DATE", mode="REQUIRED"),
        SchemaField("ticker", "STRING", mode="REQUIRED"),
        SchemaField("exchange", "STRING", mode="REQUIRED"),
        SchemaField("open", "FLOAT64", mode="NULLABLE"),
        SchemaField("high", "FLOAT64", mode="NULLABLE"),
        SchemaField("low", "FLOAT64", mode="NULLABLE"),
        SchemaField("close", "FLOAT64", mode="REQUIRED"),
        SchemaField("volume", "FLOAT64", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_ref, schema=schema)

    # Partition by date — makes queries cheaper and faster
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date"
    )

    # Cluster by exchange and ticker — optimizes filtering queries
    table.clustering_fields = ["exchange", "ticker"]

    try:
        table = bq_client.create_table(table)
        print(f"Created table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        print("  Partitioned by: date (DAY)")
        print("  Clustered by: exchange, ticker")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"Table already exists — skipping creation")
        else:
            raise e

    return table_ref

def load_gcs_to_bigquery(bq_client, gcs_client):
    """Load all CSV files from GCS into BigQuery"""
    bucket = gcs_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix="raw/stocks/"))

    csv_blobs = [b for b in blobs if b.name.endswith(".csv")]
    print(f"\nFound {len(csv_blobs)} CSV files in GCS")

    total_rows = 0
    successful = 0
    failed = []

    for blob in csv_blobs:
        ticker_name = blob.name.split("/")[-1].replace(".csv", "")
        print(f"\nLoading {ticker_name}...")
        try:
            content = blob.download_as_text()
            df = pd.read_csv(io.StringIO(content))

            if df.empty:
                print(f"  Empty file, skipping...")
                continue

            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["open"] = pd.to_numeric(df["open"], errors="coerce")
            df["high"] = pd.to_numeric(df["high"], errors="coerce")
            df["low"] = pd.to_numeric(df["low"], errors="coerce")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
            df = df.dropna(subset=["close"])

            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
            job_config = LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    SchemaField("date", "DATE"),
                    SchemaField("ticker", "STRING"),
                    SchemaField("exchange", "STRING"),
                    SchemaField("open", "FLOAT64"),
                    SchemaField("high", "FLOAT64"),
                    SchemaField("low", "FLOAT64"),
                    SchemaField("close", "FLOAT64"),
                    SchemaField("volume", "FLOAT64"),
                ]
            )

            job = bq_client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()

            rows = len(df)
            total_rows += rows
            successful += 1
            print(f"  Loaded {rows:,} rows")

        except Exception as e:
            print(f"  ERROR loading {ticker_name}: {e}")
            failed.append(ticker_name)

    return total_rows, successful, failed

def main():
    print("=" * 60)
    print("  Loading GCS Data into BigQuery")
    print(f"  Dataset: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    print("=" * 60)

    bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_PATH)
    gcs_client = storage.Client.from_service_account_json(CREDENTIALS_PATH)

    # Create table with partitioning and clustering
    create_table_if_not_exists(bq_client)

    # Load all data
    total_rows, successful, failed = load_gcs_to_bigquery(bq_client, gcs_client)

    print("\n" + "=" * 60)
    print(f"  Load Complete!")
    print(f"  Files loaded: {successful}")
    print(f"  Total rows in BigQuery: {total_rows:,}")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    print("=" * 60)

if __name__ == "__main__":
    main()