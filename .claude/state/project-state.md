<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-23 (later — U3 staging shims landed)_

## Where we are

- **Active workstream — GCP project consolidation.** Per `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` (v2), all data ops consolidates into `project-41542e21-470f-4589-96d`. `dee-data-ops-prod` + `dee-data-ops` decommissioned after 30-day stability.
- **U3 landed (2026-04-23 later):** staging layer resolves end-to-end against the consolidated project. Four blob-shims (Stripe / Typeform / Fathom / Calendly) over `Raw.<source>_objects_raw`; GHL column-rename at the source CTE only. 13 view models materialize, 77 data tests pass, PASS=90 ERROR=0. PR #69 merged as `9ef4a0b`.
- **U2 (prior):** dbt profiles + env + CI retargeted to `project-41542e21-...`. Functional `dbt debug` verified under U3 via oauth sidecar (ADC). PR #67 / commit `5d85afc`.
- **Closed workstream (unchanged):** Speed-to-Lead star-schema refactor shipped 2026-04-23 close-out. `speed_to_lead_detail` wide mart feeds 15/15 Metabase cards; dashboard live.
- **Next concrete step:** U4a plumbing parity (**HARD GATE for U5**). Freeze GTM raw snapshot + `dee-data-ops-prod` baseline; run Merge dbt end-to-end against frozen snapshot; prove 15,283-row `fct_speed_to_lead_touch` reproduces. Do not start without David's explicit sign-off on the U3 staging shape.
- **Headline metric (locked 2026-04-19):** unchanged. Must reproduce identically at U4a.

## Active plan

- **File:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — 15 units (U4 split into U4a + U4b post-U1), 4 phases, ~4 weeks active + 30-day soak.
- **Three HARD GATES:** U4a (plumbing parity — 15,283 rows reproduce from a frozen snapshot), U8 (Stripe revenue parity), U12 (identity-spine 14-day parity). U4b (live-raw business parity) is a soft/soak gate feeding U14, not a PR gate.
- **Execution cadence:** one phase at a time; phase boundaries are non-negotiable. U4b parallelizes with U6–U8 once `bq-ingest` is repaired.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-23 later** — U3 staging shims landed: 4 blob-shims (Stripe/Typeform/Fathom/Calendly) + GHL column rename; kept pre-U3 Calendly filenames (`stg_calendly__events.sql`) to preserve `fct_calls_booked` refs; abandoned the Track X Fivetran+poller dual-source; `form_id` gap logged for U9. (`grep -n "U3 staging shims" WORKLOG.md`)
- **2026-04-23 late** — U2 retarget landed: dbt profiles + env + CI now point at `project-41542e21-470f-4589-96d` for dev/ci/prod. (`grep -n "U2 retarget" WORKLOG.md`)
- **2026-04-23 evening** — U1 preflight executed; plan's U3 scope grew (+Calendly shim, +GHL column-rename) and U4 split into U4a + U4b. (`grep -n "U1 preflight executed" WORKLOG.md`)

## Open threads

- **U4a sign-off** — David's approval needed on the U3 staging shape before U4a plumbing parity begins.
- **`merge-dbt-ci@` SA + keyfile** — committed `profiles.yml` declares `method: service-account` but no SA / keyfile exists on `project-41542e21-...` (U1 preflight §12). Local dev works via oauth sidecar; CI `dbt-deploy.yml` blocked until provisioned.
- **`bq-ingest` service repair** — GTM repo work; prerequisite for U4b (not U4a). Last-known healthy Phase-2 writes 2026-04-19 14:33; scheduler returns OK but no rows land.
- **Typeform `form_id` gap** — not carried by GTM extractor; staging emits NULL; `not_null` test lifted with restore-at-U9 note.
- **GHL `conversations` undercount** — 101 rows in `raw_ghl.ghl__conversations_raw` vs 1,314 in `Raw.ghl_objects_raw`. U4a decides whether to swap to a blob-shim.
- **GHL `messages` / `users` / `tasks`** — 0 rows upstream on both per-object and blob paths; no shim can help; upstream fix not in U3/U4 scope.
- **Fathom transcript landing issue** — 0% coverage across 1,157 calls; port + fix in U6.
- **Stripe ~50-day staleness** — pre-existing GTM bug (`stripe-backfill` failing daily); tracked for U7/U8.
- **Fanbasis** — broken on both sides; separate diagnostic session.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look (retrieval map)

- **Active plan:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` (has its own "Fresh session startup" block)
- **Memories (auto-loaded):** `project_gcp_consolidation_decision.md`, `feedback_preserve_working_infra.md`, `feedback_ship_over_ceremony.md` via `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md`
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks; scope routing in `.claude/rules/using-the-notebook.md`
- **Staging shims (U3):** `dbt/models/staging/{ghl,calendly,stripe,typeform,fathom}/`
- **Existing star-schema marts:** `dbt/models/warehouse/facts/fct_speed_to_lead_touch.sql`, `dbt/models/marts/speed_to_lead_detail.sql`
- **Metabase authoring:** `ops/metabase/authoring/dashboards/speed_to_lead.py`, `ops/metabase/authoring/sync.py`
- **GTM source repo (read-only reference):** `/Users/david/Documents/operating system/Projects/GTM lead warehouse`
- **Full history:** `grep -n "^## " WORKLOG.md`; `grep -n "U3 staging shims" WORKLOG.md` for this entry
