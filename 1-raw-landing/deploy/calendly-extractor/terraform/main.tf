###############################################################################
# Track X — Calendly Extractor: Cloud Run Job + Cloud Scheduler
#
# Replaces the Fivetran Calendly connector with a custom Cloud Run Job
# polling Calendly's v2 API at 1-min cadence for 1-min freshness on
# scheduled_events, invitees, and invitee_no_shows.
#
# Design mirrors Track W (GHL extractor) exactly:
# - Single Cloud Run Job (no hot/cold split — Calendly volume is ~1-2
#   orders of magnitude below GHL; one job covers all three endpoints).
# - Same ingest SA (ingest@dee-data-ops.iam.gserviceaccount.com) — already
#   has BQ + Secret Manager IAM from Track J.
# - Shared Artifact Registry repo (`ingest`) created by Track W.
#   Import it if Track W already applied: see README.md.
# - BQ advisory lock (`raw_calendly._job_locks`) in extract.py prevents
#   scheduler-overlap double-ingest at 1-min cadence.
# - `calendly-api-token` secret in dee-data-ops-prod must be created +
#   populated by David before `terraform apply` (manual checkpoint #1).
#
# Corpus grounding:
# - Cloud Run Jobs: no standing cost, GCP-native auth, Terraform-managed.
#   Source: .claude/rules/ingest.md "Near-real-time exception (Cloud Run Jobs)",
#   Data Ops notebook. Pattern proven by Track W.
# - Single job decision: Calendly data volume is low; splitting into hot/cold
#   like GHL would add Terraform complexity with no freshness benefit.
#   (Track X decision, 2026-04-22)
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
  image = "us-central1-docker.pkg.dev/${var.project_id}/ingest/calendly-extractor:${var.image_tag}"
  # Env vars injected into the Cloud Run Job execution.
  # GCP_PROJECT_ID_DEV targets "dee-data-ops" — the raw-landing dataset lives
  # in the dev project; dbt reads cross-project from prod.
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
# Artifact Registry — shared ingest repo (created by Track W).
# Import if it already exists:
#   terraform import google_artifact_registry_repository.ingest \
#     projects/dee-data-ops-prod/locations/us-central1/repositories/ingest
# ---------------------------------------------------------------------------
resource "google_artifact_registry_repository" "ingest" {
  location      = var.region
  repository_id = "ingest"
  description   = "Container images for custom ingest extractors (GHL, Calendly, Fanbasis, etc.)"
  format        = "DOCKER"

  lifecycle {
    # Track W created this repo; prevent accidental destroy from this module.
    prevent_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Cloud Run Job — Calendly (all endpoints, every 1 min)
# Single job; no hot/cold split (Track X decision 2026-04-22).
# ---------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "calendly_poll" {
  name     = "calendly-poll"
  location = var.region

  template {
    template {
      service_account = var.ingest_sa_email
      max_retries     = 1
      timeout         = "120s"

      containers {
        image = local.image
        # All endpoints in a single run; no args needed (default = all)
        # For targeted pulls use:
        #   gcloud run jobs execute calendly-poll --args="--endpoints=scheduled_events,invitees"

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
# IAM: Cloud Scheduler SA → invoker on the Calendly job
# ---------------------------------------------------------------------------
resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes_calendly" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.calendly_poll.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.scheduler_sa_email}"
}

# ---------------------------------------------------------------------------
# Cloud Scheduler — every 1 minute
# ---------------------------------------------------------------------------
resource "google_cloud_scheduler_job" "calendly_poll" {
  name        = "calendly-poll"
  description = "Trigger calendly-poll Cloud Run Job every minute (all endpoints)"
  schedule    = "* * * * *"
  time_zone   = "UTC"
  region      = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.calendly_poll.name}:run"

    oauth_token {
      service_account_email = var.scheduler_sa_email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  retry_config {
    retry_count = 0  # don't retry; next scheduler tick fires in 60s anyway
  }

  depends_on = [
    google_cloud_run_v2_job.calendly_poll,
    google_cloud_run_v2_job_iam_member.scheduler_invokes_calendly,
  ]
}
