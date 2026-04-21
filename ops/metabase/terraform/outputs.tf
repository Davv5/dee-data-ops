output "metabase_url" {
  description = "Public URL — browse here after the VM finishes booting (~2 min)"
  value       = "https://${replace(google_compute_address.metabase.address, ".", "-")}.nip.io"
}

output "metabase_public_ip" {
  description = "Static external IP assigned to the Metabase VM"
  value       = google_compute_address.metabase.address
}

output "bq_reader_sa_email" {
  description = "Service account email — run `gcloud iam service-accounts keys create` for this SA and upload the JSON to Secret Manager (see README)"
  value       = google_service_account.metabase_bq_reader.email
}

output "bq_reader_key_secret_name" {
  description = "Secret Manager secret ID where the BQ SA key JSON gets uploaded"
  value       = google_secret_manager_secret.bq_reader_key.secret_id
}

output "cloud_sql_connection_name" {
  description = "Cloud SQL instance connection name (project:region:instance) — used by cloud_sql_proxy on the VM"
  value       = google_sql_database_instance.metabase.connection_name
}

output "ops_bucket" {
  description = "GCS bucket for startup script + nightly app-DB backups"
  value       = google_storage_bucket.metabase_ops.name
}
