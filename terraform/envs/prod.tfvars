project_id = "crypto-analytics-2026"
gcp_region = "europe-north1"
environment = "prod"

#Environment-specific variables for production environment. Override these values in staging.tfvars and prod.tfvars as needed.
bucket_lifecycle_rules = {
    age = 90
}

enable_versioning = false
dataset_friendly_name = "Crypto Analytics Prod Dataset"
dataset_description = "BigQuery dataset for crypto analytics project in production environment"