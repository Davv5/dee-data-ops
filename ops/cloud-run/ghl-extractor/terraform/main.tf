###############################################################################
# Track W — GHL Extractor: Cloud Run Jobs + Cloud Scheduler
# Migrates GHL ingest from GitHub Actions 5-min cron to Cloud Run Jobs at
# 1-min (hot) and 15-min (cold) cadence, enabling the sub-5-min STL SLA.
#
# Design rationale:
# - Cloud Run Jobs: no standing cost, GCP-native auth, same SA as GHA path.
#   (Track W corpus-deviation; rationale in .claude/rules/ingest.md)
# - BQ advisory lock in extract.py prevents scheduler-overlap double-ingest.
# - Same ingest SA as Track J; no new principal.
###############################################################################

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  image = "us-central1-docker.pkg.dev/${var.project_id}/ingest/ghl-extractor:${var.image_tag}"
  # Env vars injected into every Cloud Run Job execution.
  # GCP_PROJECT_ID_DEV stays "dee-data-ops" — the raw-landing dataset lives
  # in the dev project by design; dbt reads cross-project from prod.
  # Track W decision: do NOT rename this var here (rename is a separate concern).
  common_env = [
    {
      name  = "GCP_PROJECT_ID_DEV"
      value = "dee-data-ops"
    },
    {
      name  = "GCP_SECRET_MANAGER_PROJECT"
      value = var.project_id
    },
  ]
}

# ---------------------------------------------------------------------------
# Artifact Registry — repo for extractor images
# If Track J already created this repo, import it:
#   terraform import google_artifact_registry_repository.ingest \
#     projects/dee-data-ops-prod/locations/us-central1/repositories/ingest
# ---------------------------------------------------------------------------
resource "google_artifact_registry_repository" "ingest" {
  location      = var.region
  repository_id = "ingest"
  description   = "Container images for custom ingest extractors (GHL, Fanbasis, etc.)"
  format        = "DOCKER"

  lifecycle {
    # If Track J created this repo outside Terraform, prevent accidental destroy.
    prevent_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Cloud Run Job — GHL Hot (conversations + messages, every 1 min)
# ---------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "ghl_hot" {
  name     = "ghl-hot"
  location = var.region

  template {
    template {
      service_account = var.ingest_sa_email
      max_retries     = 1
      timeout         = "120s"

      containers {
        image = local.image
        args  = ["--endpoints", "conversations,messages"]

        dynamic "env" {
          for_each = local.common_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }

  depends_on = [google_artifact_registry_repository.ingest]
}

# ---------------------------------------------------------------------------
# Cloud Run Job — GHL Cold (contacts + opportunities + users + pipelines, every 15 min)
# ---------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "ghl_cold" {
  name     = "ghl-cold"
  location = var.region

  template {
    template {
      service_account = var.ingest_sa_email
      max_retries     = 1
      timeout         = "300s"

      containers {
        image = local.image
        args  = ["--endpoints", "contacts,opportunities,users,pipelines"]

        dynamic "env" {
          for_each = local.common_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }

  depends_on = [google_artifact_registry_repository.ingest]
}

# ---------------------------------------------------------------------------
# IAM: Cloud Scheduler SA → invoker on each Cloud Run Job
# ---------------------------------------------------------------------------
resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes_hot" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.ghl_hot.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.scheduler_sa_email}"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes_cold" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.ghl_cold.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.scheduler_sa_email}"
}

# ---------------------------------------------------------------------------
# Cloud Scheduler — Hot (every 1 minute)
# ---------------------------------------------------------------------------
resource "google_cloud_scheduler_job" "ghl_hot" {
  name        = "ghl-hot"
  description = "Trigger ghl-hot Cloud Run Job every minute (conversations + messages)"
  schedule    = "* * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri = "https://${var.region}-run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.ghl_hot.name}:run"

    oidc_token {
      service_account_email = var.scheduler_sa_email
      audience              = "https://${var.region}-run.googleapis.com/"
    }
  }

  retry_config {
    retry_count = 0  # don't retry; next scheduler tick fires in 60s anyway
  }

  depends_on = [
    google_cloud_run_v2_job.ghl_hot,
    google_cloud_run_v2_job_iam_member.scheduler_invokes_hot,
  ]
}

# ---------------------------------------------------------------------------
# Cloud Scheduler — Cold (every 15 minutes)
# ---------------------------------------------------------------------------
resource "google_cloud_scheduler_job" "ghl_cold" {
  name        = "ghl-cold"
  description = "Trigger ghl-cold Cloud Run Job every 15 min (contacts + opportunities + users + pipelines)"
  schedule    = "*/15 * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri = "https://${var.region}-run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.ghl_cold.name}:run"

    oidc_token {
      service_account_email = var.scheduler_sa_email
      audience              = "https://${var.region}-run.googleapis.com/"
    }
  }

  retry_config {
    retry_count = 1  # one retry for cold; 15-min cadence means a missed run hurts more
  }

  depends_on = [
    google_cloud_run_v2_job.ghl_cold,
    google_cloud_run_v2_job_iam_member.scheduler_invokes_cold,
  ]
}
