# Calendly extractor — Cloud Run Jobs image
#
# Build context is the repo root. Invoked as:
#   docker build -t calendly-extractor:<tag> -f ingestion/calendly/Dockerfile .
#
# Entry point accepts all flags that extract.py supports:
#   --endpoints scheduled_events,invitees,invitee_no_shows   (default: all)
#   --since 2026-04-22T00:00:00Z        (backfill override)
#   --dry-run                            (exercise BQ client without writing)
#
# Required env vars at runtime (provided via Cloud Run Job env / Secret Manager):
#   GCP_PROJECT_ID_DEV          — destination BQ project (dee-data-ops)
#   GCP_SECRET_MANAGER_PROJECT  — project hosting the secrets (dee-data-ops-prod)
#   GOOGLE_APPLICATION_CREDENTIALS — injected automatically by Cloud Run via SA binding

FROM python:3.11-slim

WORKDIR /app

# Install deps first (layer-cached when only source changes)
COPY ingestion/calendly/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the extractor package. The entrypoint uses `python -m ingestion.calendly.extract`
# so the package hierarchy must be preserved under /app.
COPY ingestion/ ./ingestion/

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "ingestion.calendly.extract"]
