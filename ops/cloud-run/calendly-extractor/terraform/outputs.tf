output "calendly_poll_job_name" {
  description = "Cloud Run Job name for the Calendly poller (all endpoints)"
  value       = google_cloud_run_v2_job.calendly_poll.name
}

output "calendly_poll_scheduler_name" {
  description = "Cloud Scheduler job name for the Calendly poller"
  value       = google_cloud_scheduler_job.calendly_poll.name
}

output "artifact_registry_repo" {
  description = "Artifact Registry repository path for the calendly extractor image"
  value       = "us-central1-docker.pkg.dev/${var.project_id}/ingest/calendly-extractor"
}

output "calendly_poll_console_url" {
  description = "GCP console URL for the Calendly Cloud Run Job"
  value       = "https://console.cloud.google.com/run/jobs/details/${var.region}/${google_cloud_run_v2_job.calendly_poll.name}?project=${var.project_id}"
}
