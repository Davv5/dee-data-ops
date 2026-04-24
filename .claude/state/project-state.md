<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-24 (Strategic Reset — Discovery Sprint begins)_

## Where we are

- **Active workstream — Strategic Reset / Data Discovery & Visibility Sprint** (2026-04-24 → target 2026-05-08). Pause new build for 1–2 weeks; map all source data + all business areas (not just Speed-to-Lead); Grok-prioritized Gold-layer roadmap; then resume build. Plan: `docs/plans/2026-04-24-strategic-reset.md`.
- **Foundation is intact (do not rebuild):** BigQuery + dbt + 13 staging models end-to-end + `(id, _ingested_at, payload)` raw-landing discipline. U1 preflight, U2 profile retarget, U3 staging shims all stay shipped.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` resumes (with U5+ warehouse scope rewritten against the Grok roadmap) after the sprint. U4a plumbing parity contract stays valid.
- **Closed workstream (unchanged):** Speed-to-Lead star-schema refactor shipped 2026-04-23. `speed_to_lead_detail` wide mart feeds 15/15 Metabase cards; dashboard live. Speed-to-Lead is now one of several business areas being (re)scoped.
- **Next concrete step:** Discovery Sprint day-1 tasks — source inventory + business-area map (see Strategic Reset Plan §7–10 day focus).
- **Headline metric (locked 2026-04-19):** unchanged. Stays as the reproduction target for U4a when cutover resumes.

## Active plan

- **Primary:** `docs/plans/2026-04-24-strategic-reset.md` — 5 parallel workstreams, 4 artifacts under `docs/discovery/`, ~1–2 weeks.
- **Paused:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — 15 units, U1–U3 complete; U4a+ resumes after Gold-layer roadmap exists.
- **Execution cadence during sprint:** docs-only. Zero dbt / warehouse / mart PRs unless the deliverable is docs. Daily one-line WORKLOG entries.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-24** — Strategic Reset: pause new build, run Discovery Sprint, rebuild Gold against Grok-prioritized roadmap. Third restart considered and rejected; foundation is sound; problem is visibility + prioritization, not tech. (`grep -n "Strategic Reset" WORKLOG.md`)
- **2026-04-23 later** — U3 staging shims landed: 4 blob-shims (Stripe/Typeform/Fathom/Calendly) + GHL column rename; kept pre-U3 Calendly filenames; abandoned Track X Fivetran+poller dual-source; `form_id` gap logged for U9. (`grep -n "U3 staging shims" WORKLOG.md`)
- **2026-04-23 late** — U2 retarget landed: dbt profiles + env + CI now point at `project-41542e21-470f-4589-96d` for dev/ci/prod. (`grep -n "U2 retarget" WORKLOG.md`)

## Open threads

- **Sprint deliverables** — `docs/discovery/{source-inventory,business-area-map,coverage-matrix,gold-layer-roadmap}.md` owed by ~2026-05-08.
- **U4a paused** — resumes when Gold-layer roadmap exists. David's sign-off on U3 staging shape still standing as the precondition to resume.
- **`merge-dbt-ci@` SA + keyfile** — paused per Strategic Reset; needed before CI can run dbt builds in prod post-resume.
- **`bq-ingest` service repair** — paused per Strategic Reset; GTM-repo work; prerequisite for U4b when cutover resumes.
- **Typeform `form_id` gap** — paused per Strategic Reset; real fix is U9.
- **GHL `conversations` undercount** (101 vs 1,314 blob) — paused per Strategic Reset; U4a decision when cutover resumes.
- **GHL `messages` / `users` / `tasks`** — 0 rows upstream on both per-object and blob paths; upstream extractor fix out of sprint + cutover scope.
- **Fathom transcript landing** — 0% coverage across 1,157 calls; paused per Strategic Reset; fix scheduled for U6 post-resume.
- **Stripe ~50-day staleness** — pre-existing GTM bug; moot until Gold roadmap decides how much historical Stripe matters (live payments = Fanbasis).
- **Fanbasis** — broken on both sides; elevated priority because it's the live revenue source; scoping picks up in Discovery Sprint §business-area-map.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look (retrieval map)

- **Active plan:** `docs/plans/2026-04-24-strategic-reset.md`
- **Paused plan:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`
- **Sprint artifacts (in progress):** `docs/discovery/` (directory created when first artifact lands)
- **Memories (auto-loaded):** `project_gcp_consolidation_decision.md`, `feedback_preserve_working_infra.md`, `feedback_ship_over_ceremony.md`, `project_stripe_historical_only.md` via `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md`
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks; scope routing in `.claude/rules/using-the-notebook.md`
- **U1 preflight (seed for source inventory):** `docs/_archive/gtm-gcp-inventory.md`
- **Staging shims (U3, frozen):** `2-dbt/models/staging/{ghl,calendly,stripe,typeform,fathom}/`
- **GTM source repo (read-only reference):** `/Users/david/Documents/operating system/Projects/GTM lead warehouse`
- **Full history:** `grep -n "^## " WORKLOG.md`; `grep -n "Strategic Reset" WORKLOG.md` for this pivot
