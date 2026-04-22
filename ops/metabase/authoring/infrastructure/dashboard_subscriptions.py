"""Assert desired state on the Speed-to-Lead weekly email digest.

Creates or updates a Metabase dashboard subscription (API name: "pulse")
that emails the full Speed-to-Lead dashboard as a PDF attachment every
Monday at 06:00 in the instance's configured report-timezone.

SMTP PREREQUISITE — this script will exit 1 with a clear message if SMTP
is not yet configured on the Metabase instance. Check SMTP status with::

    python -c "
    from ops.metabase.authoring.client import MetabaseClient
    from ops.metabase.authoring.infrastructure.dashboard_subscriptions import check_smtp
    check_smtp(MetabaseClient())
    "

As of 2026-04-22, SMTP is NOT configured on https://34-66-7-243.nip.io.
See the SMTP bootstrap note at the bottom of this docstring for the
steps David needs to take before this script can run end-to-end.

TIMEZONE PREREQUISITE — ``schedule_hour = 6`` is interpreted in the
instance's report-timezone (``Admin → Settings → Localization → Report
Timezone``). As of 2026-04-22, report-timezone is null (system default).
Before creating the pulse, this script asserts
``report-timezone = America/New_York`` per David's instruction (D-DEE
operates US Eastern hours). Pre-change value noted in the verification
output so the commit message + WORKLOG can document it.

Corpus citations:
- *"Dashboard subscriptions"* (Metabase Learn notebook, source 9fe1ca85)
  — subscriptions can email any address, even non-Metabase users.
- *"Pushing data"* (Metabase Learn notebook, source 46e8daaf) — "only push
  data that changes behaviour" + "match the frequency of pushing a metric
  with the cadence of the decision-making." SDR-management decisions play
  out over a week, so weekly is the correct cadence.
- /api/pulse endpoint confirmed via the API endpoints table (source
  4e547d7e).

Recipient list is read from env var STL_WEEKLY_DIGEST_RECIPIENTS
(comma-separated). Script fails loudly if unset — the recipient list is
not a sensible default; David supplies it.

Run (after SMTP is configured)::

    source .venv/bin/activate
    set -a && source ops/metabase/.env.metabase && set +a
    python -m ops.metabase.authoring.infrastructure.dashboard_subscriptions

Re-running is a no-op when state already matches.

----------------------------------------------------------------------
SMTP BOOTSTRAP (one-time, David must complete before this script works)
----------------------------------------------------------------------

Step 1 — Choose an SMTP provider.
  Recommended: SendGrid free tier (100 emails/day free, API key auth).
  Alternative: Gmail SMTP relay (requires 2FA + App Password).

Step 2 — Store creds in Secret Manager.
  Per .claude/rules/metabase.md Rule 3, credentials go in GCP Secret
  Manager (dee-data-ops-prod), NOT in .env.metabase.

  For SendGrid::

      gcloud secrets create metabase-smtp-password \\
          --data-file=<(echo -n "<your-sendgrid-api-key>") \\
          --project=dee-data-ops-prod

  Set::
      metabase-smtp-host   = smtp.sendgrid.net
      metabase-smtp-port   = 587
      metabase-smtp-user   = apikey
      metabase-smtp-pass   = <SendGrid API key> (stored above)
      metabase-smtp-from   = notifications@precisionscaling.io (or your domain)

Step 3 — Configure Metabase SMTP via Admin UI or /api/email PUT.
  The /api/email endpoint returned 404 on this instance (OSS 60.x does
  not expose a writable /api/email at that path for non-Cloud builds).
  Use Admin → Settings → Email in the Metabase GUI to fill in the SMTP
  details. This is a one-time bootstrap, consistent with how the BQ
  connection was bootstrapped.

Step 4 — Verify by sending a test email from the Admin UI
  (Admin → Settings → Email → "Send test email").

Step 5 — Re-run this script. The SMTP guard will pass and the pulse will
  be created.
----------------------------------------------------------------------
"""
from __future__ import annotations

import os
import sys

from ..client import MetabaseClient

DASHBOARD_NAME = "Speed-to-Lead"
SUBSCRIPTION_NAME = "Speed-to-Lead -- weekly digest"

# Monday 06:00 weekly. schedule_hour is in the instance's report-timezone
# (asserted to America/New_York by this script before creating the pulse).
SCHEDULE = {
    "schedule_type": "weekly",
    "schedule_day": "mon",
    "schedule_hour": 6,
    "schedule_frame": None,
}

DESIRED_REPORT_TIMEZONE = "America/New_York"


# ── SMTP guard ───────────────────────────────────────────────────────────

def check_smtp(mb: MetabaseClient) -> None:
    """Exit 1 with a helpful message if SMTP is not configured.

    Reads email-configured? from GET /api/setting (list) because the
    individual /api/email endpoint returns 404 on this OSS build.
    """
    all_settings = mb.get("/setting")
    email_configured = next(
        (s.get("value") for s in all_settings if s.get("key") == "email-configured?"),
        False,
    )
    if not email_configured:
        print(
            "ERROR: SMTP is not configured on this Metabase instance.\n"
            "email-configured? = False\n\n"
            "Dashboard subscriptions (pulses) require SMTP to deliver emails.\n"
            "See the 'SMTP BOOTSTRAP' section in this file's docstring for\n"
            "the steps David needs to complete before this script can run.\n\n"
            "Item 1 (caching_config.py) is unaffected — run that separately.",
            file=sys.stderr,
        )
        sys.exit(1)


# ── Timezone assert ──────────────────────────────────────────────────────

def assert_report_timezone(mb: MetabaseClient) -> str:
    """Assert report-timezone = America/New_York. Return the pre-change value."""
    all_settings = mb.get("/setting")
    before = next(
        (s.get("value") for s in all_settings if s.get("key") == "report-timezone"),
        None,
    )
    print(f"  report-timezone before: {before!r}  want: {DESIRED_REPORT_TIMEZONE!r}", end=" ")
    if before == DESIRED_REPORT_TIMEZONE:
        print("-- ok")
    else:
        mb.put("/setting/report-timezone", {"value": DESIRED_REPORT_TIMEZONE})
        print("-- PUT")
    return before  # caller logs this in WORKLOG


# ── Recipients ───────────────────────────────────────────────────────────

def _recipients() -> list[dict]:
    raw = os.environ.get("STL_WEEKLY_DIGEST_RECIPIENTS", "").strip()
    if not raw:
        print(
            "ERROR: STL_WEEKLY_DIGEST_RECIPIENTS env var is unset.\n"
            "Set it in ops/metabase/.env.metabase (comma-separated list of\n"
            "email addresses), then re-run.\n"
            "Recipient list is NOT a sensible default -- David supplies it.",
            file=sys.stderr,
        )
        sys.exit(2)
    emails = [e.strip() for e in raw.split(",") if e.strip()]
    return [{"email": e} for e in emails]


# ── Dashboard lookup ─────────────────────────────────────────────────────

def _find_dashboard(mb: MetabaseClient, name: str) -> dict:
    matches = [d for d in mb.dashboards() if d.get("name") == name]
    if not matches:
        raise LookupError(
            f"No Metabase dashboard named {name!r}. Ship "
            "ops/metabase/authoring/dashboards/speed_to_lead.py first."
        )
    # Prefer the publicly-shared copy.
    public = [d for d in matches if d.get("public_uuid")]
    best = public[0] if public else matches[0]
    return mb.get(f"/dashboard/{best['id']}")


# ── Pulse lookup ─────────────────────────────────────────────────────────

def _find_pulse(mb: MetabaseClient, name: str) -> dict | None:
    # GET /api/pulse returns a list of pulses visible to the current user.
    # Match on name -- pulses are not collection-scoped on OSS, so name alone.
    pulses = mb.get("/pulse")
    if isinstance(pulses, list):
        return next((p for p in pulses if p.get("name") == name), None)
    return None


# ── Main ─────────────────────────────────────────────────────────────────

def main() -> None:
    mb = MetabaseClient()

    # Guard 1: SMTP must be configured.
    check_smtp(mb)

    # Guard 2: Recipient list must be set.
    recipients = _recipients()

    # Assert timezone before creating the pulse so schedule_hour=6 fires
    # at 06:00 America/New_York, not 06:00 UTC (which would be ~01:00-02:00 ET).
    print("Asserting report-timezone:")
    tz_before = assert_report_timezone(mb)

    # Locate the dashboard.
    dash = _find_dashboard(mb, DASHBOARD_NAME)

    # Build pulse_cards: every dashcard that has a real card_id.
    dashcards = [dc for dc in dash.get("dashcards", []) if dc.get("card_id")]
    pulse_cards = [
        {
            "id": dc["card_id"],
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": dc["id"],
        }
        for dc in dashcards
    ]

    # Flip include_csv=True on the leaderboard card so leadership gets raw
    # numbers they can slice in Excel. Matches by name substring -- robust
    # to card_id churn across re-runs of speed_to_lead.py.
    LEADERBOARD_SUBSTRING = "leaderboard"
    for pc, dc in zip(pulse_cards, dashcards):
        try:
            card = mb.get(f"/card/{pc['id']}")
            if LEADERBOARD_SUBSTRING in card.get("name", "").lower():
                pc["include_csv"] = True
        except Exception:
            pass  # if card lookup fails, default include_csv=False is safe

    channel = {
        "channel_type": "email",
        "enabled": True,
        "recipients": recipients,
        **SCHEDULE,
    }

    payload = {
        "name": SUBSCRIPTION_NAME,
        "cards": pulse_cards,
        "channels": [channel],
        "dashboard_id": dash["id"],
        "skip_if_empty": False,
        "parameters": [],  # inherit dashboard default filters
    }

    existing = _find_pulse(mb, SUBSCRIPTION_NAME)
    if existing:
        print(f"Updating pulse {SUBSCRIPTION_NAME!r} (id={existing['id']}).")
        mb.put(f"/pulse/{existing['id']}", payload)
    else:
        print(f"Creating pulse {SUBSCRIPTION_NAME!r}.")
        mb.post("/pulse", payload)

    # Verify.
    after = _find_pulse(mb, SUBSCRIPTION_NAME)
    assert after is not None, "Pulse not found after upsert"
    ch = after["channels"][0]
    print(
        f"  schedule: {ch.get('schedule_type')}/{ch.get('schedule_day')} "
        f"@ {ch.get('schedule_hour'):02d}:00"
    )
    print(f"  recipients: {[r['email'] for r in ch.get('recipients', [])]}")
    print(f"  cards: {len(after.get('cards', []))}")
    print(f"\nreport-timezone before this run: {tz_before!r} (document in WORKLOG)")
    print("Done.")


if __name__ == "__main__":
    main()
