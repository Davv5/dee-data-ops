output "ghl_hot_job_name" {
  description = "Cloud Run Job name for the hot group (conversations + messages)"
  value       = google_cloud_run_v2_job.ghl_hot.name
}

output "ghl_cold_job_name" {
  description = "Cloud Run Job name for the cold group (contacts + opportunities + users + pipelines)"
  value       = google_cloud_run_v2_job.ghl_cold.name
}

output "ghl_hot_scheduler_name" {
  description = "Cloud Scheduler job name for the hot group"
  value       = google_cloud_scheduler_job.ghl_hot.name
}

output "ghl_cold_scheduler_name" {
  description = "Cloud Scheduler job name for the cold group"
  value       = google_cloud_scheduler_job.ghl_cold.name
}

output "artifact_registry_repo" {
  description = "Artifact Registry repository path for the ingest extractor image"
  value       = "us-central1-docker.pkg.dev/${var.project_id}/ingest/ghl-extractor"
}

output "ghl_hot_console_url" {
  description = "GCP console URL for the hot Cloud Run Job"
  value       = "https://console.cloud.google.com/run/jobs/details/${var.region}/${google_cloud_run_v2_job.ghl_hot.name}?project=${var.project_id}"
}

output "ghl_cold_console_url" {
  description = "GCP console URL for the cold Cloud Run Job"
  value       = "https://console.cloud.google.com/run/jobs/details/${var.region}/${google_cloud_run_v2_job.ghl_cold.name}?project=${var.project_id}"
}
