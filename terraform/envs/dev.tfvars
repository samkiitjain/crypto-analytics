project_id = "crypto-analytics-2026"
gcp_region = "europe-north1"
environment = "dev"

#Environment-specific variables for development environment. Override these values in staging.tfvars and prod.tfvars as needed.
bucket_lifecycle_rules = {
    age = 30
}
  
enable_versioning = false
dataset_friendly_name = "Crypto Analytics Dev Dataset"
dataset_description = "BigQuery dataset for crypto analytics project in development environment"