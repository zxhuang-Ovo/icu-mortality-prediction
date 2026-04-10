-- Export vitals from BigQuery to GCS (Google Cloud Storage)
EXPORT DATA OPTIONS(
  uri='gs://icu-data-export-2026/vitals/vitals-*.parquet',
  format='PARQUET'
)
AS
SELECT * FROM dataset.vitals_long_48h;

-- Export labs from BigQuery to GCS
EXPORT DATA OPTIONS(
  uri='gs://icu-data-export-2026/labs/labs-*.parquet',
  format='PARQUET'
)
AS
SELECT * FROM dataset.labs_long_48h;
