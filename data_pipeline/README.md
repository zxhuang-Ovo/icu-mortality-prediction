# Data Pipeline

This module handles data extraction from BigQuery.

## Workflow

BigQuery → GCS → Local

## Steps

1. Export data from BigQuery

2. Download using gsutil

3. Delete cloud storage to avoid cost

## Notes

- Data is stored in Parquet format
- Do NOT upload raw data to GitHub
