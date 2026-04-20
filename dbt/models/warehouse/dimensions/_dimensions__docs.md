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
