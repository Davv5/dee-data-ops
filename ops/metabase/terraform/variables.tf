variable "project_id" {
  description = "GCP project hosting Metabase + the BigQuery marts"
  type        = string
  default     = "dee-data-ops-prod"
}

variable "region" {
  description = "GCP region for Metabase VM + Cloud SQL"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the Metabase VM (must be in var.region)"
  type        = string
  default     = "us-central1-a"
}

variable "vm_machine_type" {
  description = "GCE machine type for the Metabase VM. e2-small handles v1 load; bump to e2-medium if >20 concurrent users."
  type        = string
  default     = "e2-small"
}

variable "cloud_sql_tier" {
  description = "Cloud SQL tier for Metabase's app-DB. db-f1-micro is enough for v1."
  type        = string
  default     = "db-f1-micro"
}

variable "allowed_oauth_domains" {
  description = "Google OAuth email domains permitted to log into Metabase. Configured at Metabase bootstrap, not at the IAP layer."
  type        = list(string)
  default     = ["gmail.com"]
}

variable "bq_marts_dataset" {
  description = "BigQuery dataset Metabase reads. In D-DEE this is marts; for a new client rename."
  type        = string
  default     = "marts"
}
