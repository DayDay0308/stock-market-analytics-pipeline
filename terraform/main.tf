terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# GCS Bucket - Data Lake
resource "google_storage_bucket" "stock_data_lake" {
  name          = "${var.project}-stock-data-lake"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset - Data Warehouse
resource "google_bigquery_dataset" "stock_dataset" {
  dataset_id            = var.bq_dataset
  project               = var.project
  location              = var.region
  delete_contents_on_destroy = true
}