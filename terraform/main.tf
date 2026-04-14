locals {
  name_prefix = "crypto-analytics-${var.environment}"
  common_labels = {
    environment = var.environment
    managed_by  = "terraform"
    purpose     = "crypto-analytics"
  }
}

#Raw data landing zone - where raw data is ingested and stored in its original format
resource "google_storage_bucket" "raw_data" {
    name = "${local.name_prefix}-raw-data"
    location = var.gcp_region
    labels = local.common_labels
    force_destroy = true

    uniform_bucket_level_access = true

    lifecycle_rule {
      condition {
        age = var.bucket_lifecycle_rules
      }

      action {
        type = "Delete"
      }
    }

    versioning {
      enabled = var.enable_versioning
    }
 
}

resource "google_bigquery_dataset" "crypto_analytics" {
  for_each = var.bq_datasets

  project = var.project_id
  dataset_id    = replace("${local.name_prefix}_${each.value}", "-", "_")
  location      = var.gcp_region
  labels        = local.common_labels
  description   = var.dataset_description
  friendly_name = var.dataset_friendly_name

  delete_contents_on_destroy = false
}