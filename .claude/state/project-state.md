# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 (Phase B.2 ships: bridge is Fanbasis-aware + `fct_revenue` renamed to `fct_payments`)_

## Where we are

- **Phase B (Layer Build) is live.** Three tickets shipped today:
  - PR #84 (`5776a7b`) — `stg_fanbasis__transactions` view + `fct_revenue` Stripe+Fanbasis union (Fanbasis: 466 rows / $170,712 gross / 9 refunds, Apr 2025 → Apr 2026)
  - PR #86 (`9db0899`) — `bridge_identity_contact_payment` extended to UNION Stripe + Fanbasis with composite PK `(source_platform, payment_id)` + `tier_unmatched` refactored as anti-join (no payment can be silently dropped); 5 new tests (`bridge_payment_count_parity`, `bridge_match_rate_floor`, `accepted_range` on match_score, `relationships` on contact_sk, `unique_combination_of_columns` on the composite PK). **Match rates: Stripe 94.22%, Fanbasis 99.36%** in dev_david
  - PR #88 (`1f5c73b`) — `fct_revenue` → `fct_payments` semantic rename (grain stays "one row per payment event"; the rename describes the actual grain instead of an aggregate)
- **Existing wide marts auto-widened post-bridge:** `revenue_detail` now attributes Fanbasis revenue per contact_sk; `lead_journey` revenue rollups now include Fanbasis on next refresh; `sales_activity_detail` and `speed_to_lead_detail` are revenue-independent and unchanged.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path; raw `notebook_query` calls are reserved for ad-hoc lookups.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 14 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` U4a+ resumes when GHL trusted-copy decision lands.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **Sprint (closed):** `docs/plans/2026-04-24-strategic-reset.md` — Phase A complete; Phase B build is live.
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28** — Phase B.2 closes the rank-3 (`revenue_detail` Refresh) path of the gold-layer roadmap. Bridge extended with anti-join `tier_unmatched` (every payment guaranteed one bridge row) + composite PK; `fct_revenue` renamed to `fct_payments` to describe payment-event grain. Destination: PRs #86 (`9db0899`) + #88 (`1f5c73b`).
- **2026-04-28** — Phase B.1 ships: `stg_fanbasis__transactions` (1:1 staging view, `payment_id` PK with unique/not_null tests) + the warehouse fact becomes a real Stripe+Fanbasis union, NOT a separate `fct_fanbasis_transactions` (one fact per business process per `.claude/rules/warehouse.md`). Refund-amount modeling deferred (binary `is_refunded` only). Destination: PR #84 (`5776a7b`).
- **2026-04-27** — Phase A → Phase B transition: ship the agent-kit `data-engineer` agent + LAW-skill catalog as v1; close the Strategic Reset; authorize Phase B build work. Destination: `docs/decisions/2026-04-27-phase-a-to-b-transition.md` (ADR) + PR #80.

## Open threads

- **Next Phase B candidates** (pick from these, none currently in flight):
  - **`fct_refunds`** — Fanbasis refunds entity. Staging needs extension to unnest `$.refunds` JSON array (currently only `is_refunded` boolean). 9 of 466 Fanbasis rows have non-empty refunds today. Independent of GHL.
  - **`fct_opportunity_stage_transitions`** — first confirm raw landing carries stage-change events with timestamps. Blocker class: `dbt-staging` audit, then warehouse-fct-scaffold.
  - **`dim_typeform_form`** — blocked on Typeform `form_id` gap (every response has NULL `form_id` upstream — U9 extractor work). `stg_typeform__forms` doesn't exist yet.
  - **Period-grain rollups** — `revenue_detail_by_week`, `lead_journey_by_month`, etc. on top of existing wide marts. No new staging dependencies.
- **GHL trusted-copy decision** — single named blocker for several Tier B/refresh marts. Choose between legacy blob (1,314 conversation rows) and Phase-2 (101 rows). Resolves 92% undercount + four empty entities + dual-source ambiguity in one move.
- **GCP IAM hygiene (cosmetic, not blocking).** `GCP_SA_KEY` in repo secrets carries `ingest@dee-data-ops.iam.gserviceaccount.com`; the kit-purpose `sa-transform@project-41542e21-470f-4589-96d.iam.gserviceaccount.com` is the right home for dbt-CI auth. Future hygiene PR: rotate the secret + update the workflow header.
- **Local dev env stale** — `.env` still points `GCP_PROJECT_ID_DEV=dee-data-ops`; consolidated project is `project-41542e21-470f-4589-96d`. Manual override needed every dbt run. Worth retargeting CLAUDE.local.md + .env at session start.
- **Fathom → GHL contact join key** — attendee email reliability. Affects future `fct_calls_held`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional. Lock after first production queries.
- **`bq-ingest` service repair**, **Typeform `form_id` gap**, **GHL `messages` / `users` / `tasks` 0-row upstream**, **Fathom transcript landing**, **Stripe staleness** — all paused per Strategic Reset; revisit on cutover resume.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look

- **Canonical roadmap (on main):** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` "Mart architecture commitment" + `.claude/rules/mart-naming.md` Rule 2
- **Phase B artifacts (on main):** `2-dbt/models/staging/fanbasis/stg_fanbasis__transactions.sql` + `_fanbasis__models.yml`; `2-dbt/models/warehouse/facts/fct_payments.sql`; `2-dbt/models/warehouse/bridges/bridge_identity_contact_payment.sql` + `_bridges__models.yml` + `_bridges__docs.md`; `2-dbt/tests/{bridge_payment_count_parity,bridge_match_rate_floor}.sql`
- **Existing wide marts (auto-widened post-bridge):** `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Data-engineer agent + LAW skills:** `~/.claude/agents/data-engineer.md` owns engagement lifecycle; specialist seams via `altimate-{sql-review,data-parity,schema-migration,dbt-unit-tests}` skills. Discoverability rule: `.claude/rules/use-data-engineer-agent.md`. Hooks: PreToolUse(Write|Edit) → `pre-sql-altimate-review.sh`; PostToolUse(Write|Edit) → `post-sql-qa-baseline.sh`.
- **Corpus engine v2:** `.claude/skills/ask-corpus/scripts/` (engine) + `.claude/skills/ask-corpus/SKILL.md` (voice contract) + `SKILL-v1.md` (backup)
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Shared portable kit:** `/Users/david/Documents/agent-kit` (installed globally via `~/.claude/agents/data-engineer.md` symlink + 16 `~/.claude/skills/<kit-name>` symlinks; project-imported via `import-agent-kit.sh --symlink`)
- **Codex parity:** `AGENTS.md` + `.agents/skills/{ask-corpus,skill-creator,worklog}/`
- **Sprint artifacts:** `docs/discovery/{source-inventory,source-shapes,staging-models,gap-analysis,insights-summary,business-area-map,coverage-matrix,gold-layer-roadmap}.md`
- **Memories (auto-loaded):** see `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `.claude/rules/*.md`
- **Routing rule for end-of-session:** `.claude/rules/worklog.md`
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-28 (post Phase B.2 ship)
- WORKLOG: skipped — Phase B.2 narrative captured in PR #86 + #88 descriptions and commit messages; bridge match-rate evidence in PR #86 body; rename mechanics in PR #88 body. No residual content needing append-only narration.
