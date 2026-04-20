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
