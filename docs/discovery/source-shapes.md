# Source Shapes - Builder Spec

**Purpose:** This page is the buildable spec behind `docs/discovery/coverage-matrix.md`. For each confirmed data source, it names the tables and events we need, the fields per table, the acceptance criteria, the blocker class, and which business questions unblock when each piece lands.

Last updated: 2026-04-25.

The Coverage Matrix is for decisions ("can we answer this question yet?"). This page is for the builder ("what exactly do I have to land for that to flip from yellow to green?"). The two docs are paired - one is plain-English and decision-led, the other is field-level and build-led.

---

## How to read this page

For each source:

- **Status** - is this source live, partial, or blocked.
- **Auth state** - what we have today, what we still need, who owes it.
- **Public API docs** - the canonical reference URL where shape can be confirmed. If empty, docs are still a client or vendor ask.
- **Unblocks across the matrix** - which question rows are gated on this source.

Then per table or event we need from that source:

- **Shape** - what kind of object it is and how it lands.
- **Fields needed** - the specific columns or payload keys required for the matrix to work.
- **Acceptance** - the test that says "this is done" - usually a row count, freshness threshold, or non-null rate.
- **Blocker class** - one of the six tags below.
- **Unblocks** - which Coverage Matrix cells flip when this lands.

### Blocker classes

Use these tags consistently so you can grep the doc for "what can I pull forward today vs what's waiting on someone else":

- **`client-access`** - D-DEE must grant credentials, share a secret, or run a setting change in their tenant.
- **`vendor-support`** - the data vendor (Fanbasis, Fathom, etc.) has to confirm shape, ship a fix, or upgrade a plan tier.
- **`extractor-build`** - work lives in `1-raw-landing/<source>/`. David builds.
- **`dbt-staging`** - work lives in `2-dbt/models/staging/<source>/`. David builds.
- **`dbt-warehouse`** - work lives in `2-dbt/models/warehouse/`. David builds.
- **`client-decision`** - D-DEE must choose between options (which GHL copy to trust, whether Slack is data, etc.).

Phase A note: this document is the spec. Build work happens in Phase B, after the Gold-layer roadmap is approved.

---

## Calendly

- **Status:** Live and fresh. Used for booking grain.
- **Auth state:** Live via Fivetran free tier.
- **Public API docs:** developer.calendly.com - scheduled_events and invitees endpoints.
- **Unblocks across the matrix:** Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q10, Q11.

### scheduled_events

- **Shape:** One row per booking event. Fivetran-loaded.
- **Fields needed:** `event_uuid`, `created_at`, `start_time`, `end_time`, `status` (values include `active`, `canceled`), event-type reference, host (closer/setter assigned), invitee email and phone for join to GHL contact.
- **Acceptance:** ≥99% of last-30-day events have non-null `status`; canceled rows present (not just active).
- **Blocker class:** `dbt-staging` (status field not fully landed in current staging).
- **Unblocks:** Q7 (no-show / cancel / reschedule analysis); tightens Q1, Q5, Q11.

### invitee_no_shows

- **Shape:** Calendly emits no-show as a webhook event and marks `cancellation` on the invitee record.
- **Fields needed:** `event_uuid`, `invitee_email`, `no_show_at`, `cancellation.reason`, `cancellation.canceler_type`.
- **Acceptance:** No-show events for last 30 days landed and joinable to scheduled_events.
- **Blocker class:** `extractor-build` if not in current Fivetran scope; otherwise `dbt-staging`.
- **Unblocks:** Q7.

### event_types

- **Shape:** Reference table. One row per Calendly event type definition.
- **Fields needed:** `event_type_uri`, `name`, `slug`, `kind` (solo / group / collective).
- **Acceptance:** Reference table joinable to scheduled_events on event-type URI.
- **Blocker class:** `dbt-staging`.
- **Unblocks:** None alone; supports event-type breakdowns on Q1, Q5, Q7.

---

## GHL (GoHighLevel)

- **Status:** Live but stale. Two data copies in play; trusted copy not yet chosen.
- **Auth state:** PIT in `GHL_API_KEY` GitHub secret; location ID `yDDvavWJesa03Cv3wKjt` in `GHL_LOCATION_ID`. Initial PIT was exposed in a Claude transcript on 2026-04-19 - rotate via GHL Settings → Private Integrations and update the secret before next CI run.
- **Public API docs:** highlevel.stoplight.io - v2 API reference.
- **Unblocks across the matrix:** every question except Q13.

### contacts

- **Shape:** One row per contact in the GHL location. Custom fields land as nested JSON; raw-landing is JSON-string per the schema-drift-proof decision.
- **Fields needed:** `id`, `email`, `phone`, `dateAdded`, `tags`, `customFields` (parsed via `JSON_VALUE`), source / UTM fields, owner / assigned user.
- **Acceptance:** Freshness ≤24h; row count within ±5% of older copy when reconciled.
- **Blocker class:** `client-decision` (which GHL copy is trusted) → `extractor-build` (rebuild against trusted source).
- **Unblocks:** Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12.

### conversations

- **Shape:** One row per conversation thread. `lastMessageType` and `lastManualMessageDate` are the speed-to-lead signal.
- **Fields needed:** `id`, `contactId`, `assignedTo` (joins to SDR roster seed), `lastMessageType` (filter to `TYPE_CALL`, `TYPE_SMS`), `lastMessageDate`, `lastManualMessageDate` (the human-vs-automation lever).
- **Acceptance:** Row count within ±5% of older copy; `lastManualMessageDate` populated where it should be.
- **Blocker class:** `client-decision` → `extractor-build`.
- **Unblocks:** Q1 (Speed-to-Lead is the load-bearing column), Q3, Q7, Q12.

### opportunities

- **Shape:** One row per opportunity (deal). Carries pipeline + pipeline_stage references.
- **Fields needed:** `id`, `contactId`, `pipelineId`, `pipelineStageId`, `status` (open / won / lost / abandoned), `monetaryValue`, `assignedTo`, stage transition timestamps.
- **Acceptance:** Stage history available with timestamps - this is the gate for Q6 (time stuck per step).
- **Blocker class:** `vendor-support` (confirm whether GHL emits stage transition events or only current-state) → `extractor-build` if event-stream needed.
- **Unblocks:** Q2, Q3, Q4, Q5, Q6, Q8, Q9, Q10, Q11.

### pipelines + pipeline_stages

- **Shape:** Reference tables. The `is_booked_stage` flag lives on `dim_pipeline_stages` per the Speed-to-Lead grain decision.
- **Fields needed:** `pipelineId`, `name`, `stages[]` with `id`, `name`, `position`.
- **Acceptance:** `is_booked_stage` flag set on the correct stage per pipeline (the open spec from the Speed-to-Lead lock - resolve with David).
- **Blocker class:** `client-decision` → `dbt-warehouse`.
- **Unblocks:** Q1, Q6.

### users

- **Shape:** One row per GHL user.
- **Fields needed:** `id`, `name`, `email`, `roles`.
- **Acceptance:** Non-empty - currently empty in source.
- **Blocker class:** `vendor-support` (confirm endpoint returns rows for this tenant).
- **Unblocks:** Q3, Q5 (closer identity), Q12.

### tasks + messages

- **Shape:** Activity tables for follow-up cadence analysis.
- **Fields needed:** `id`, `contactId`, `dueDate` or `sentAt`, `type`, `status`.
- **Acceptance:** Non-empty - currently empty in source.
- **Blocker class:** `vendor-support` → `extractor-build`.
- **Unblocks:** Q3, Q7.

---

## Fanbasis

- **Status:** Live revenue source. Transactions landing in raw. No cleaned-up staging table. Refund / chargeback / customer / subscription shapes unverified.
- **Auth state:** TBD - Week-0 client ask still open per scope §7. CSV fallback documented.
- **Public API docs:** Pending - the single highest-leverage docs ask on this engagement. Request from D-DEE / Fanbasis support as part of Week-0.
- **Unblocks across the matrix:** Q2, Q4, Q5, Q8, Q9, Q11.

### transactions

- **Shape:** Inspect raw payloads to confirm. Raw-landing JSON-string row per the schema-drift-proof decision.
- **Fields needed:** transaction id, customer identifier (joinable to GHL contact via email or phone), amount, currency, status, created_at, payment method, product / plan reference if available.
- **Acceptance:** ≥99% of last-30-day paid contacts joinable to a GHL contact; cleaned-up staging table exists; refund / chargeback events distinguishable from successful charges.
- **Blocker class:** `vendor-support` (confirm payload shape from API docs) → `extractor-build` → `dbt-staging`.
- **Unblocks:** Q2, Q4, Q5, Q11.

### refunds

- **Shape unknown:** Could be (a) separate event rows, (b) status updates on the original transaction, (c) webhook events, or some combination. Confirming the shape is the precondition for Q8 staging design.
- **Fields needed:** original transaction reference, refund amount, refund timestamp, refund reason if available.
- **Acceptance:** Refund events for last 30 days landed and joinable to original transaction.
- **Blocker class:** `vendor-support` (this is the single most important docs question on the matrix).
- **Unblocks:** Q8.

### chargebacks / disputes

- **Shape unknown:** Same uncertainty as refunds. May share a single events table or live separately.
- **Fields needed:** original transaction reference, chargeback amount, chargeback timestamp, status (won / lost / pending), reason code if available.
- **Acceptance:** Chargeback events landed and joinable to original transaction.
- **Blocker class:** `vendor-support`.
- **Unblocks:** Q8.

### customers / subscriptions

- **Shape unknown:** Whether Fanbasis carries a recurring-customer or subscription entity beyond transaction-level data is unverified.
- **Fields needed:** customer id, contact identifier (email or phone joinable to GHL), `first_paid_at`, status (active / churned / paused), plan or product reference.
- **Acceptance:** Customer-level retention measurable - the gate for Q9.
- **Blocker class:** `vendor-support`.
- **Unblocks:** Q9.

---

## Typeform

- **Status:** Live and fresh on responses; form-ID gap on the cleaned-up table; no Typeform forms reference table yet.
- **Auth state:** Live via Fivetran free tier per scope.
- **Public API docs:** developer.typeform.com - Responses and Forms endpoints.
- **Unblocks across the matrix:** Q2, Q4, Q10, Q11.

### responses

- **Shape:** One row per submission. Fivetran-loaded.
- **Fields needed:** `response_id`, `form_id` (the gap), `landed_at`, `submitted_at`, `hidden` fields for UTM passthrough, answers payload, contact identifier (email or phone for GHL join).
- **Acceptance:** ≥95% of last-30-day responses have non-null `form_id` and non-null contact identifier.
- **Blocker class:** `dbt-staging` if `form_id` is in the raw payload but dropped in staging; `extractor-build` if Fivetran connector isn't requesting it; `vendor-support` if Typeform doesn't surface it via the connector path being used.
- **Unblocks:** Q2, Q4, Q10, Q11.

### forms

- **Shape:** Reference table. One row per Typeform form definition.
- **Fields needed:** `form_id`, `title`, `created_at`, `last_updated_at`, `published`, optional tag or category metadata.
- **Acceptance:** Reference table exists and joinable to responses on `form_id`.
- **Blocker class:** `extractor-build` (forms endpoint is separate from responses) → `dbt-staging`.
- **Unblocks:** Q4, Q10, Q11.

---

## Fathom

- **Status:** Call basics exist. Transcripts missing for all calls.
- **Auth state:** TBD - Week-0 client ask.
- **Public API docs:** Confirm via Fathom support - the public API surface is limited and transcript access is plan-tier-gated.
- **Unblocks across the matrix:** Q5 (partly), Q12.

### calls

- **Shape:** One row per recorded call.
- **Fields needed:** `call_id`, `meeting_url` or Calendly event reference, `recorded_at`, attendees, duration, host (closer).
- **Acceptance:** Call basics joinable to a Calendly scheduled_event row for at least the closer cohort.
- **Blocker class:** `client-access` (Fathom API access) → `extractor-build`.
- **Unblocks:** Q5 partly. Without transcripts, only volume + length analysis.

### transcripts

- **Shape:** Per-call transcript with speaker turns and timestamps.
- **Fields needed:** `call_id` reference, speaker label (closer vs prospect), text, timestamp.
- **Acceptance:** ≥80% of last-30-day closer calls have a transcript landed and parseable.
- **Blocker class:** `vendor-support` (confirm Fathom plan tier includes API transcript access for D-DEE's account) → `client-decision` (whether to upgrade plan if not included) → `extractor-build`.
- **Unblocks:** Q12 (the call-content blocker).

---

## Stripe

- **Status:** Banned. Historical-only. Frozen.
- **Auth state:** TBD - Week-0 client ask, but only relevant if historical lookback gets ranked into a build list.
- **Public API docs:** stripe.com/docs/api - reference for shape if the historical bridge is built.
- **Unblocks across the matrix:** Q2, Q4, Q8, Q9, Q11 - historical lookback only. Stripe does not unblock anything for current-state reporting.

### checkout_sessions

- **Shape:** Already partial-loaded (4,750 sessions per the Stripe connector gap noted in CLAUDE.local.md).
- **Fields needed:** `session_id`, `customer`, `amount_total`, `created`, `payment_status`.
- **Acceptance:** Stripe connector gap resolved if and only if revenue attribution becomes a v2+ need.
- **Blocker class:** `client-decision` (do we need historical revenue blended with current?) → `vendor-support` (Fivetran connector gap on `customer`, `charge`, `invoice`, `payment_intent`).
- **Unblocks:** Historical lookback for Q2, Q4, Q11 only.

### customers

- **Shape:** Stripe customer entity. Currently zero rows landing per the Fivetran connector gap.
- **Fields needed:** `customer_id`, `email`, `phone`, `created`, `metadata` (especially the GHL contact identifier if D-DEE wrote one in).
- **Acceptance:** Bridge from Stripe customer to current Fanbasis customer measurable.
- **Blocker class:** `client-decision` (do we need the bridge?) → `vendor-support` (Fivetran connector fix) → `dbt-warehouse`.
- **Unblocks:** Historical Q9 retention; otherwise nothing.

### disputes / refunds

- **Shape:** Stripe dispute and refund objects. Connector gap also applies here.
- **Fields needed:** dispute id, charge reference, amount, status, reason, created.
- **Acceptance:** Same as customers - only relevant if historical refund analysis is explicitly ranked into the build list.
- **Blocker class:** `client-decision`.
- **Unblocks:** Historical Q8 only.

---

## Slack

Out of scope. Slack is an alert destination, not a data source, per current evidence. If David later confirms Slack messages or client communications must be reported on, this section gets populated; until then the matrix excludes Slack and so does this spec.

- **Blocker class to flip status:** `client-decision`.
- **Trigger to act:** explicit confirmation from David that Slack data matters for Q13.

---

## What this means for the build sequence

Reading the blocker-class column across all sources tells you what you can pull forward today vs what you have to chase down.

### Pullable today (work lives in your repo)

- **GHL contacts and opportunities staging refresh** - once trusted copy is chosen. `client-decision` then `dbt-staging`.
- **Calendly status field landing** - pure `dbt-staging`. Unblocks Q7 directly.
- **Calendly event_types reference** - `dbt-staging`. Tightens Q1, Q5, Q7.
- **Typeform forms reference table** - `extractor-build` then `dbt-staging`. Unblocks attribution.

### Waiting on D-DEE or vendor

- **Fanbasis API docs** - `vendor-support` via D-DEE. Single highest-leverage unblock; gates Q2, Q4, Q5, Q8, Q9, Q11.
- **GHL trusted-copy decision** - `client-decision` with David. Single highest-leverage internal unblock.
- **Fathom transcript plan-tier confirmation** - `vendor-support` then `client-decision`. Gates Q12.
- **PIT rotation** - `client-access` (already flagged 2026-04-19).

### Explicitly deferred until ranked

- **Stripe connector gap** - only fix if historical revenue blending is ranked into a build list.
- **Slack as data** - only act if David confirms Q13 needs answering as data, not governance.

---

## Keeping this in sync with the Coverage Matrix

When a Coverage Matrix cell flips status (yellow to green, or blocked to working), update the corresponding source-shapes acceptance criterion in this file in the same commit. The two docs are paired: the matrix shows what we can decide; this page shows what was built or unblocked to make that decision possible.

When the next discovery page (`docs/discovery/dashboard-build-list.md`) is written, it should pull its prerequisites directly from this file's "Pullable today" and "Waiting on" lists - those become the build list's ordered phases.
