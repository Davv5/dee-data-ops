<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-23 (late — U2 retarget landed)_

## Where we are

- **Active workstream — GCP project consolidation.** Per `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` (v2), all data ops consolidates into `project-41542e21-470f-4589-96d` (GTM's GCP project, formerly "My First Project"). `dee-data-ops-prod` + `dee-data-ops` decommissioned after 30-day stability.
- **U2 landed (2026-04-23 late):** dbt `profiles.yml`, `.env.example`, `.github/workflows/dbt-deploy.yml`, `dbt_project.yml`, and `macros/generate_schema_name.sql` retargeted to `project-41542e21-470f-4589-96d`. All three targets (dev/ci/prod) now default to the consolidated project.
- **Why:** GTM project has 2 years of live ingestion — 17 Cloud Run Jobs, 6 sources, trained BQML classifier, fresh raw data — that Merge lacked. David chose to preserve working infrastructure over naming hygiene.
- **Closed workstream (unchanged):** Speed-to-Lead star-schema refactor shipped 2026-04-23 close-out. `speed_to_lead_detail` wide mart feeds 15/15 Metabase cards; 11 legacy `stl_*` rollups dropped. Dashboard live.
- **Next concrete step:** U3 staging shims (Stripe / Typeform / Fathom / Calendly) + GHL column-rename so staging resolves against GTM's current raw shape.
- **Headline metric (locked 2026-04-19):** unchanged. Must reproduce identically in `project-41542e21-...` at U4a plumbing parity gate.

## Active plan

- **File:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — 15 units (U4 split into U4a + U4b post-U1), 4 phases, ~4 weeks active + 30-day soak.
- **Three HARD GATES:** U4a (plumbing parity — 15,283 rows reproduce from a frozen snapshot), U8 (Stripe revenue parity), U12 (identity-spine 14-day parity). U4b (live-raw business parity) is a soft/soak gate feeding U14, not a PR gate.
- **Execution cadence:** one phase at a time; phase boundaries are non-negotiable. U4b parallelizes with U6–U8 once `bq-ingest` is repaired.
- **Plan v1** (port extractors into `dee-data-ops-prod`) was discarded 2026-04-23 pm; flipped to consolidate-in-GTM-project.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-23 late** — U2 retarget landed: dbt profiles + env + CI now point at `project-41542e21-470f-4589-96d` for dev/ci/prod. `dbt compile` verified against the new target; no behaviour change until staging shims land in U3. (`grep -n "U2 retarget" WORKLOG.md`)
- **2026-04-23 evening** — U1 preflight executed; plan's U3 scope grew (+Calendly shim, +GHL column-rename) and U4 split into U4a (frozen plumbing parity, HARD GATE for U5) + U4b (live-raw business parity, parallel, gates U14). (`grep -n "U1 preflight executed" WORKLOG.md`)
- **2026-04-23 pm** — GCP project consolidation into `project-41542e21-...`; extractors migrate to Merge's `ingestion/` on first touch (U6/U7/U9). (`grep -n "GCP project consolidation" WORKLOG.md`)

## Open threads

- **U1 preflight sign-off** — awaits David review on 4 items in `docs/preflight/gtm-gcp-inventory.md` §13: (a) GHL staleness handling, (b) SA provisioning, (c) Calendly-shim scope expansion, (d) Stripe pre-existing staleness acknowledgement.
- **`bq-ingest` service repair** — GTM repo work; prerequisite for U4b but not U2–U4a. Last-known healthy writes: 2026-04-20 17:20 (legacy), 2026-04-19 14:33 (Phase-2). Scheduler returns HTTP 0, no rows land.
- **GHL Phase-2 partial coverage** — `messages/notes/tasks/users` empty in `raw_ghl`; U3 plan defaults to additional blob-shim against `Raw.ghl_objects_raw` for each.
- **Fathom transcript landing issue** — U1 confirmed 0% transcript coverage across 1,157 calls. Port + fix in U6.
- **Stripe ~50-day staleness** — pre-existing GTM bug; `stripe-backfill` failing daily. Tracked for U7/U8; not cutover-blocking.
- **Secret Manager keyfile for Merge CI** — not provisioned. U2 retargeted config to `project-41542e21-...` but `GCP_SA_KEY_PROD` repo secret still needs rotation to a SA with BQ Data Editor + Job User on the new project before `dbt-deploy.yml` will succeed. CI/CD blocked until then.
- **Track X operational bringup (Calendly poller)** — unresolved from prior session; may overlap consolidation.
- **PR #50 (Track E merge conflicts), #44 (curl hardening)** — open from earlier sessions.
- **Fanbasis** — broken on both sides; separate diagnostic session.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look (retrieval map)

- **Active plan:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` (has its own "Fresh session startup" block)
- **Memories (auto-loaded):** `project_gcp_consolidation_decision.md`, `feedback_preserve_working_infra.md`, `feedback_ship_over_ceremony.md` via `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md`
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks; scope routing in `.claude/rules/using-the-notebook.md`
- **Existing star-schema marts:** `dbt/models/warehouse/facts/fct_speed_to_lead_touch.sql`, `dbt/models/marts/speed_to_lead_detail.sql`
- **Metabase authoring:** `ops/metabase/authoring/dashboards/speed_to_lead.py`, `ops/metabase/authoring/sync.py`
- **GTM source repo (read-only reference):** `/Users/david/Documents/operating system/Projects/GTM lead warehouse`
- **Full history:** `grep -n "^## " WORKLOG.md`; `grep -n "GCP project consolidation" WORKLOG.md` for this decision
