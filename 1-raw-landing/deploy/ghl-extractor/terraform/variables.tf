variable "project_id" {
  description = "GCP project that hosts Cloud Run Jobs + Cloud Scheduler (dee-data-ops-prod)"
  type        = string
  default     = "dee-data-ops-prod"
}

variable "region" {
  description = "GCP region for Cloud Run Jobs and Cloud Scheduler"
  type        = string
  default     = "us-central1"
}

variable "image_tag" {
  description = "Container image tag to deploy. CI sets this to the short git SHA; local apply uses 'latest'."
  type        = string
  default     = "latest"
}

variable "ingest_sa_email" {
  description = "Service account that Cloud Run Jobs run as (already has Secret Manager + BQ IAM from Track J)"
  type        = string
  default     = "ingest@dee-data-ops.iam.gserviceaccount.com"
}

variable "scheduler_sa_email" {
  description = "Service account that Cloud Scheduler uses to invoke Cloud Run Jobs (needs roles/run.invoker)"
  type        = string
  default     = "cloud-scheduler@dee-data-ops-prod.iam.gserviceaccount.com"
}
