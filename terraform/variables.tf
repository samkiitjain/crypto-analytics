variable "project_id" {
    description = "The ID of the project to deploy resources in."
    type        = string
}

variable "gcp_region" {
  description = "GCP region of the service."
  type = string
}

variable "environment" {
    description = "The environment to deploy to (e.g., dev, staging, prod)."
    type        = string
    validation {
      condition = contains(["dev", "staging", "prod"], var.environment)
      error_message = "Environment must be either dev, staging or prod"
    }
}

variable "bucket_lifecycle_rules" {
  description = "List of lifecycle rules for the storage bucket. Each rule should be a map with 'age' and 'action' keys."
  type = number
  default = 90
  
}

variable "enable_versioning" {
  description = "Whether to enable versioning on the storage bucket."
  type = bool
  default = false
}

variable "dataset_friendly_name" {
  description = "Friendly name for the BigQuery dataset."
  type = string
  default = "Crypto Analytics Dataset"
}

variable "dataset_description" {
  description = "Description for the BigQuery dataset."
  type = string
  default = "BigQuery dataset for crypto analytics project."
}