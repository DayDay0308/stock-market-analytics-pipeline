# Stock Market Analytics Pipeline

## Overview

This project implements an end-to-end batch data engineering pipeline that ingests, processes, and analyzes global stock market data across 6 exchanges. It transforms raw financial data into analytics-ready datasets and delivers actionable insights through an interactive dashboard.

The pipeline simulates a real-world data platform used by analysts and investors to monitor market trends, compare exchange performance, and track stock behavior over time.

## Business Problem

Financial market data is highly fragmented and difficult to analyze across different exchanges. Investors and analysts often struggle to:

- Compare stock performance across global markets
- Identify trends and volatility patterns
- Access clean, structured historical data for analysis

This project addresses these challenges by building a centralized analytics platform that provides cross-exchange performance comparison, trend analysis using moving averages, and volatility and price change insights.

## Architecture

yfinance API → Python Ingestion → GCS Data Lake → BigQuery Data Warehouse → dbt Transformations → Looker Studio Dashboard

- Data is ingested daily via the yfinance API as CSV files
- Raw data is stored in Google Cloud Storage (GCS) as the data lake
- Data is loaded into BigQuery, partitioned by date and clustered by exchange and ticker
- Transformations are handled using dbt to produce analytics-ready tables
- Insights are visualized in Looker Studio

## Tech Stack

- Cloud Platform: Google Cloud Platform (GCP)
- Infrastructure as Code: Terraform
- Orchestration: Kestra
- Data Lake: Google Cloud Storage (GCS)
- Data Warehouse: BigQuery (partitioned by date, clustered by exchange and ticker)
- Transformations: dbt (Data Build Tool)
- Visualization: Looker Studio
- Language: Python 3.12

## Dataset

- Exchanges: NYSE (USA), JSE (South Africa), LSE (UK), TSE (Japan), EURONEXT (Europe), SSE (China)
- Stocks: ~300 tickers across 6 global exchanges
- Time Range: 5 years of daily OHLCV data
- Volume: 300,000+ rows (~25-50MB), growing daily with new market data ingestion
- Data Fields: date, ticker, exchange, open, high, low, close, volume

## Key Features

- Automated daily batch pipeline using Kestra
- Scalable cloud-native architecture on GCP
- Optimized BigQuery tables with date partitioning and exchange/ticker clustering
- Modular dbt transformation layer with staging and core models
- Cross-exchange analytics for global market comparison
- Fully reproducible pipeline with infrastructure as code

## dbt Models

### Staging Layer

- stg_stocks: Cleans raw data, standardizes data types, and prepares data for transformation

### Core Layer

- fct_stock_daily: Daily stock metrics including price change, percentage change, 7-day moving average, 30-day moving average, and 30-day volatility. Partitioned by date and clustered by exchange and ticker.
- fct_exchange_summary: Aggregated daily metrics per exchange including average price, total trading volume, and average volatility.

## Dashboard

The Looker Studio dashboard provides:

1. Global Exchange Distribution — visualizes the number of tracked equities per exchange (categorical)
2. Market Trend Analysis — shows average closing price trends over time across exchanges (temporal)

Dashboard link: (to be added after deployment)

## Orchestration

The pipeline is orchestrated using Kestra and runs daily at 1 AM.

Workflow steps:
1. ingest_stock_data — fetches data from yfinance and uploads raw CSV files to GCS
2. load_to_bigquery — loads data from GCS into BigQuery

Pipeline definition: kestra/pipeline.yml

## Setup and Reproduction

### Prerequisites

- GCP account with billing enabled
- Python 3.12+
- Terraform installed
- dbt-bigquery installed

### Step 1 - Clone the repository

git clone https://github.com/DayDay0308/stock-market-analytics-pipeline.git
cd stock-market-analytics-pipeline

### Step 2 - Set up GCP credentials

- Create a GCP project
- Go to IAM and Admin > Service Accounts
- Create a service account with Editor role
- Download the JSON key
- Save it as credentials.json in the project root

### Step 3 - Provision infrastructure with Terraform

cd terraform
terraform init
terraform apply

This creates a GCS bucket for the data lake and a BigQuery dataset for the data warehouse.
After applying, CSV files should appear in your GCS bucket under raw/stocks/ after ingestion.

### Step 4 - Install Python dependencies

pip3 install yfinance google-cloud-storage google-cloud-bigquery pandas db-dtypes pyarrow

### Step 5 - Run data ingestion

python3 ingestion/ingest_stocks.py

This fetches 5 years of daily OHLCV data for ~300 stocks across 6 exchanges and uploads CSV files to GCS.
Expected output: CSV files appearing in GCS under raw/stocks/YYYY-MM-DD/

### Step 6 - Load data into BigQuery

python3 ingestion/load_to_bigquery.py

This loads all CSV files from GCS into a partitioned and clustered BigQuery table.
Expected output: raw_stocks table populated in BigQuery with 300,000+ rows.

### Step 7 - Run dbt transformations

cd dbt
dbt run
dbt test

This creates the staging view and core fact tables in BigQuery.
Expected output: stg_stocks, fct_stock_daily, and fct_exchange_summary tables in BigQuery.

### Step 8 - View the dashboard

The Looker Studio dashboard is accessible at: (link to be added)

## Challenges and Learnings

- Handling inconsistent ticker formats across 6 different exchanges
- Some JSE tickers are delisted and not available on yfinance — pipeline handles these gracefully
- Optimizing BigQuery cost using date partitioning and exchange/ticker clustering
- Designing dbt models with a clear staging and core separation for reusability
- Managing timezone differences in stock market data across global exchanges

## Future Improvements

- Switch from batch to streaming ingestion using Kafka
- Add more financial indicators such as RSI and MACD
- Use Parquet instead of CSV for better performance and storage efficiency
- Implement CI/CD for dbt models using GitHub Actions
- Add alerting for pipeline failures

## Project Structure

stock-market-analytics-pipeline/
- terraform/ — Infrastructure as Code (GCS bucket and BigQuery dataset)
- ingestion/ — Python ingestion and loading scripts
- kestra/ — Kestra orchestration pipeline definition
- dbt/ — dbt transformations
  - models/staging/ — Raw data cleaning and typing
  - models/core/ — Business logic and analytical models
- docker-compose.yml — Kestra local setup reference
- .env.example — Environment variables template
- README.md — Project documentation