project_id = "crypto-analytics-2026"
gcp_region = "europe-north1"
environment = "staging"

#Environment-specific variables for staging environment. Override these values in prod.tfvars as needed.
bucket_lifecycle_rules = 60
bq_datasets = {
    raw     = "raw"
    staging = "staging"
    marts   = "marts"
}
enable_versioning = false
dataset_friendly_name = "Crypto Analytics Staging Dataset"
dataset_description = "BigQuery dataset for crypto analytics project in staging environment"