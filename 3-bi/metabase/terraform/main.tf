terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # State lives in a dedicated GCS bucket so two operators don't stomp each
  # other. Bucket is provisioned once by hand (documented in README.md) — it
  # cannot be created by the same Terraform that depends on it.
  backend "gcs" {
    bucket = "dee-data-ops-prod-tfstate"
    prefix = "metabase"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# ──────────────────────────────────────────────────────────────────────────
# Service accounts
# ──────────────────────────────────────────────────────────────────────────

# The VM runs as this identity. No BQ access — only Secret Manager + logs.
resource "google_service_account" "metabase_vm" {
  account_id   = "metabase-vm"
  display_name = "Metabase VM runtime"
}

# Metabase's BigQuery data source uses this SA. Separate from the VM SA so
# that compromising the VM host doesn't hand over BQ data access directly.
resource "google_service_account" "metabase_bq_reader" {
  account_id   = "metabase-bq-reader"
  display_name = "Metabase BigQuery reader"
}

# BQ permissions per Metabase docs: Data Viewer + Metadata Viewer + Job User.
# Scope: whole project for now. Tighten to just the marts dataset post-v1 if
# cost audit ever needs it (`google_bigquery_dataset_iam_member`).
resource "google_project_iam_member" "bq_data_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.metabase_bq_reader.email}"
}

resource "google_project_iam_member" "bq_metadata_viewer" {
  project = var.project_id
  role    = "roles/bigquery.metadataViewer"
  member  = "serviceAccount:${google_service_account.metabase_bq_reader.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.metabase_bq_reader.email}"
}

# VM SA needs to read secrets + write logs + optionally pull from Cloud SQL
resource "google_project_iam_member" "vm_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.metabase_vm.email}"
}

resource "google_project_iam_member" "vm_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.metabase_vm.email}"
}

resource "google_project_iam_member" "vm_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.metabase_vm.email}"
}

# ──────────────────────────────────────────────────────────────────────────
# Secret Manager — BQ reader SA key (generated out-of-band; see README)
# ──────────────────────────────────────────────────────────────────────────

resource "google_secret_manager_secret" "bq_reader_key" {
  secret_id = "metabase-bq-reader-key"

  replication {
    auto {}
  }
}

# The secret VERSION is NOT provisioned here. David generates the key with
# `gcloud iam service-accounts keys create` and uploads it as a new version
# (see 3-bi/metabase/README.md). Rotating the key = new version + VM reboot.

# ──────────────────────────────────────────────────────────────────────────
# Cloud SQL Postgres (Metabase's app-DB)
# ──────────────────────────────────────────────────────────────────────────

resource "random_password" "metabase_db_password" {
  length  = 32
  special = false
}

resource "google_secret_manager_secret" "metabase_db_password" {
  secret_id = "metabase-db-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "metabase_db_password" {
  secret      = google_secret_manager_secret.metabase_db_password.id
  secret_data = random_password.metabase_db_password.result
}

resource "google_sql_database_instance" "metabase" {
  name             = "metabase-appdb"
  database_version = "POSTGRES_15"
  region           = var.region

  deletion_protection = true

  settings {
    tier              = var.cloud_sql_tier
    availability_type = "ZONAL"
    disk_size         = 10
    disk_autoresize   = true

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "03:00"
    }

    # VM reaches Cloud SQL over the cloud_sql_proxy (no public IP required)
    ip_configuration {
      ipv4_enabled = false
      private_network = google_compute_network.metabase.id
    }

    insights_config {
      query_insights_enabled = true
    }
  }

  depends_on = [
    google_service_networking_connection.private_vpc_connection,
  ]
}

resource "google_sql_database" "metabase" {
  name     = "metabase"
  instance = google_sql_database_instance.metabase.name
}

resource "google_sql_user" "metabase" {
  name     = "metabase"
  instance = google_sql_database_instance.metabase.name
  password = random_password.metabase_db_password.result
}

# ──────────────────────────────────────────────────────────────────────────
# VPC + private service access for Cloud SQL
# ──────────────────────────────────────────────────────────────────────────

resource "google_compute_network" "metabase" {
  name                    = "metabase"
  auto_create_subnetworks = true
}

resource "google_compute_global_address" "private_ip_range" {
  name          = "metabase-private-ip-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.metabase.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.metabase.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_range.name]
}

# ──────────────────────────────────────────────────────────────────────────
# Static external IP + firewall
# ──────────────────────────────────────────────────────────────────────────

resource "google_compute_address" "metabase" {
  name   = "metabase-public-ip"
  region = var.region
}

resource "google_compute_firewall" "allow_https" {
  name    = "metabase-allow-https"
  network = google_compute_network.metabase.name

  allow {
    protocol = "tcp"
    ports    = ["443", "80"] # 80 only so Caddy can complete the HTTP-01 ACME challenge
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["metabase"]
}

# SSH for break-glass only; consider removing once IAP SSH is configured.
resource "google_compute_firewall" "allow_ssh_iap" {
  name    = "metabase-allow-ssh-iap"
  network = google_compute_network.metabase.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  # IAP's netblock — not the whole internet.
  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["metabase"]
}

# ──────────────────────────────────────────────────────────────────────────
# GCE VM
# ──────────────────────────────────────────────────────────────────────────

resource "google_compute_instance" "metabase" {
  name         = "metabase"
  machine_type = var.vm_machine_type
  zone         = var.zone
  tags         = ["metabase"]

  boot_disk {
    initialize_params {
      # Container-Optimized OS — Google-maintained, boots into Docker.
      image = "projects/cos-cloud/global/images/family/cos-stable"
      size  = 20
    }
  }

  network_interface {
    network = google_compute_network.metabase.name

    access_config {
      nat_ip = google_compute_address.metabase.address
    }
  }

  service_account {
    email  = google_service_account.metabase_vm.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    # COS reads this key and runs the container declared in it on boot.
    # startup-script.sh lives at 3-bi/metabase/runtime/startup-script.sh
    # and is uploaded to GCS at apply time (see README).
    startup-script-url = "gs://${google_storage_bucket.metabase_ops.name}/startup-script.sh"

    # The VM reads these at boot to construct its config.
    METABASE_DB_INSTANCE       = google_sql_database_instance.metabase.connection_name
    METABASE_DB_NAME           = google_sql_database.metabase.name
    METABASE_DB_USER           = google_sql_user.metabase.name
    METABASE_DB_PASSWORD_SECRET = google_secret_manager_secret.metabase_db_password.secret_id
    METABASE_BQ_KEY_SECRET     = google_secret_manager_secret.bq_reader_key.secret_id
    METABASE_HOSTNAME          = "${replace(google_compute_address.metabase.address, ".", "-")}.nip.io"
  }

  allow_stopping_for_update = true

  depends_on = [
    google_sql_user.metabase,
    google_secret_manager_secret.bq_reader_key,
  ]
}

# ──────────────────────────────────────────────────────────────────────────
# Ops bucket — holds startup script + nightly app-DB backups
# ──────────────────────────────────────────────────────────────────────────

resource "google_storage_bucket" "metabase_ops" {
  name          = "${var.project_id}-metabase-ops"
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true
}

resource "google_storage_bucket_iam_member" "vm_bucket_rw" {
  bucket = google_storage_bucket.metabase_ops.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.metabase_vm.email}"
}
