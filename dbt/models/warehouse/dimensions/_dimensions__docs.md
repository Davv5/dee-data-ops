{% docs dim_sdr__overview %}

SDR-role-filtered conformed dimension — a role-scoped subset of `dim_users`,
not a duplicate. Carries `sdr_sk` (hashed over `user_id` identically to
`dim_users.user_sk` so the two are interchangeable for joins) plus `active_from`
/ `active_to` from the `dim_users_snapshot` SCD-2 open row.

### Why a separate dim_sdr?

"You can still use the same dimensions... it's very common, it's called a
conformed dimension." (source: "Creating a Data Model w/ dbt: Facts", Data
Ops notebook.) The rename from `user_sk` → `sdr_sk` in the mart context is
for business-readability in the wide mart (`speed_to_lead_detail` in F2) and
for self-documenting FK names in `fct_speed_to_lead_touch`. The underlying
hash is the same so F2 can join either way.

### Current-state filtering (F1)

F1 filters `is_active = true`. Inactive SDRs (departed reps) are excluded.
F2 will evaluate whether departed-SDR history should flow through via the
snapshot-based as-of join.

### Roster gaps

Roster-gap users (Ayaan, Jake, Moayad, Halle) do not appear in `dim_sdr`
because their roles in `dim_users` are currently `unknown`, not `SDR`.
Their touches in `fct_speed_to_lead_touch` carry `sdr_sk = NULL` and
`attribution_quality_flag = 'role_unknown'`. Resolving their roles is pending
David's decision (per `.claude/state/project-state.md`).

{% enddocs %}

{% docs dim_source__overview %}

Lead-source conformed dimension. One row per distinct `lead_source` value
surfaced in `dim_contacts`, plus an `__unknown__` sentinel row for NULL values.
Enriched with human-readable descriptions and paid/organic flags from the
`stl_lead_source_lookup` seed.

### Actual lead_source values

The `lead_source` field in GHL contacts carries campaign / content-label names
(e.g. "ig blueprint case study", "AI Brand Prompts", "dbb-ig") rather than a
controlled channel taxonomy. The seed covers the inferrable channel-level values
(TikTok, Instagram, YouTube, outbound, email, skool, etc.). The majority of
campaign-specific labels are in the dim with `is_paid = NULL` and are flagged
in WORKLOG "Open threads" for David to classify.

This is the correct design: the warehouse dimension takes control of the raw
source values and provides the business-layer taxonomy, rather than letting the
mart repeat the free-text logic every time.
(source: "3 Reasons Data Modeling Gets So Much Attention", Data Ops notebook —
"you can take control... to reflect better what the business is".)

### __unknown__ sentinel

The `__unknown__` row ensures `source_sk` is never NULL in
`fct_speed_to_lead_touch`. Contacts with `lead_source IS NULL` in `dim_contacts`
resolve to this row.

{% enddocs %}

{% docs dim_pipeline_stages__overview %}

`dim_pipeline_stages` flattens the nested `stages` JSON array stored on
each row of `stg_ghl__pipelines` into one row per (pipeline, stage)
pair. Upstream, GHL's `/opportunities/pipelines` endpoint returns a
pipeline per row with `stages[]` as an embedded array; the staging
layer preserves that shape as `stages_json` so this dim owns the
unnest.

The dim carries one synthesized attribute — `is_booked_stage` — that
identifies GHL-native "booked" states for downstream analytics. The
rule set is intentionally permissive:

- `lower(stage_name) like '%booked%'` — the primary case (stages
  literally named "Booked", "Booked Call", "Booked — Pending", …)
- `stage_name in ('Set', 'Set/Triage', 'Call Booked', 'Booked Call')` —
  secondary booked-adjacent labels captured from the oracle
  Revenue-by-Stage audit (2026-04-20)

### Why `is_booked_stage` is a dim attribute, not a metric grain

The oracle's explicit "Booked" stage alone tallied ~1,645 leads;
booked-adjacent stages add ~180 more. Calendly, the system-of-record
for bookings, tallied 3,141 events in the same window. The two
numbers measure different things:

- **Calendly-grain** (3,141) = every confirmed booking event,
  including leads booked before they existed as GHL opportunities
  and bookings that never made it into a GHL pipeline.
- **GHL-native booked-stage** (~1,825) = the subset of the funnel that
  reached a booked stage inside a GHL pipeline. Complementary, not
  redundant.

Per the DataOps 2026-04-20 grain audit (see `CLAUDE.local.md`
"Locked metric" table), the Speed-to-Lead denominator grain is
Calendly event — `fct_calls_booked` — and `is_booked_stage` hangs off
that fact as a joined-in dim attribute, not as a filter on the fact
definition itself.

### Joins

- To `fct_calls_booked`: via the opportunity's `pipeline_id` +
  `pipeline_stage_id` → `pipeline_stage_sk`. Surfaces whether the
  booked Calendly event also reached a booked GHL stage (diagnostic).
- To `fct_outreach` (Track E): same join path; lets the mart filter
  outbound touches by whether the contact's opportunity is currently
  sitting in a booked stage.

{% enddocs %}

{% docs dim_contacts__overview %}

`dim_contacts` is the v1 identity spine — anchored on GHL
(`stg_ghl__contacts`) per `.claude/rules/warehouse.md`. Surrogate
key is `generate_surrogate_key(['location_id', 'contact_id'])`; every
downstream fact resolves contact attribution through this key.

### `attribution_era`

Per-contact bucket — `utm` vs `pre_utm` — intended for era-aware mart
joins where UTM-tagged traffic and pre-UTM traffic need to be analyzed
separately (e.g. show-rate by lead magnet). Today the derivation uses
`lead_source` pattern matching (paid/ads keywords → `utm`; everything
else → `pre_utm`) because the Typeform-to-GHL email-join bridge is
not yet in staging. When that bridge lands, the derivation tightens
to "has any matched Typeform `hidden_utm_*` → `utm`" without changing
the column contract.

### Typeform enrichment placeholder

The Typeform-response join is structurally present but resolves to all
nulls today — `stg_typeform__responses` has no email column, so the
join axis is missing. The columns (`utm_source`, `utm_medium`,
`utm_campaign`, `latest_typeform_at`, `psychographic_score`,
`has_typeform_utm`) are kept so marts can compile against the final
shape today and light up once the answer-level pivot model ships.

### Identity matching for non-GHL sources

Per rule: multi-source bridges resolve to the GHL `contact_id`
*upstream* of `dim_contacts` (in staging or intermediate), never by
widening this dim's PK. `bridge_identity_contact_payment` is the
first such bridge and hangs off `fct_revenue`, not `dim_contacts`.

{% enddocs %}

{% docs dim_users__overview %}

`dim_users` is the single users dim for the warehouse — a unification
of what the earlier v1 plan split into `dim_sdrs` and `dim_aes`. The
change was made in Track D's grain-audit reconciliation: role filters
belong at the mart (where the Speed-to-Lead numerator actually fires),
not in the warehouse fact or dim. That keeps `fct_outreach` reusable
across SDR, AE, and Closer cuts.

### Role sourcing

`role` comes from `ghl_sdr_roster.csv` — the human-maintained seed
that supplies the SDR/AE distinction GHL itself does not provide.
Users with no roster row get `role = 'unknown'`. The metric numerator
(first outbound human SDR touch) returns zero for any lead touched
exclusively by `unknown`-role users — forcing function for keeping
the roster current.

### SCD2 via `dim_users_snapshot`

Role and email are tracked historically in `dim_users_snapshot` with
a `check` strategy on `['role', 'email']`. When David reclassifies a
user (e.g. Blagoj's dual-role resolution, Ayaan / Jake's pending role
confirmation, Moayad's departed-status addition), the snapshot closes
the old row (`dbt_valid_to` set) and opens a new one. Mart-layer
as-of joins use `dim_users_snapshot.dbt_valid_from <= event_at <
coalesce(dim_users_snapshot.dbt_valid_to, 'infinity')`.

{% enddocs %}

{% docs dim_offers__overview %}

Tiny lookup dim — currently two rows covering the D-DEE Core offer
and its payment-plan SKU. Hand-coded inline (UNION ALL in the model
body) rather than as a seed because the offer stack is small, rarely
changes, and living in SQL keeps the change history beside the other
warehouse model edits instead of requiring `dbt seed` runs.

{% enddocs %}

{% docs dim_calendar_dates__overview %}

A standard `dbt_utils.date_spine` from 2024-01-01 through
`current_date() + 1 day`, widened with year/quarter/month/week/day
attributes and an `is_weekday` flag. Used for date-bucketed mart joins
and Looker Studio filter panels.

{% enddocs %}
