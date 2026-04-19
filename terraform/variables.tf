variable "credentials" {
  description = "Path to GCP credentials file"
  default     = "../credentials.json"
}

variable "project" {
  description = "GCP Project ID"
  default     = "zoomcamp-project-493610"
}

variable "region" {
  description = "GCP Region"
  default     = "US"
}

variable "bq_dataset" {
  description = "BigQuery Dataset ID"
  default     = "stock_market_data"
}