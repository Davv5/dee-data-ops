# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 (Phase B build is live: PR #84 shipped `stg_fanbasis__transactions` + the real `fct_payments` Stripe+Fanbasis union)_

## Where we are

- **Phase B (Layer Build) is live.** First ticket shipped: PR #84 (merged `5776a7b`) — `stg_fanbasis__transactions` view + `fct_payments.sql` placeholder CTE replaced with a real `ref()`. Fanbasis arm is the live forward-going revenue contributor (466 historical rows / $170,712 gross / 9 refunds, Apr 2025 → Apr 2026); Stripe arm preserved untouched for historical rows.
- **Next Phase B ticket (queued):** extend `bridge_identity_contact_payment` with a Fanbasis-aware match-method (UNION-ALL Fanbasis arm matched on `fan.email` / `fan.phone` against existing tier hierarchy). Without it, Fanbasis rows in `fct_payments` surface as `bridge_status='unmatched'`. Independent of GHL trusted-copy decision.
- **Existing wide marts auto-widen on Fanbasis staging:** `lead_journey`, `revenue_detail`, `sales_activity_detail`, `speed_to_lead_detail` are shipped; refresh-only work after the bridge ticket lands so contact attribution flows.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path; raw `notebook_query` calls are reserved for ad-hoc lookups.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 13 staging models + 14th now (`stg_fanbasis__transactions`) + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` U4a+ resumes when GHL trusted-copy decision lands.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **Sprint (closed):** `docs/plans/2026-04-24-strategic-reset.md` — Phase A complete; Phase B build is live.
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28** — Phase B.1 ships: `stg_fanbasis__transactions` (1:1 staging view, `payment_id` PK with unique/not_null tests) + `fct_payments` becomes the real Stripe+Fanbasis union, NOT a separate `fct_fanbasis_transactions` (one fact per business process per `.claude/rules/warehouse.md`). Decision-side: bridge extension correctly deferred to a separate ticket; refund-amount modeling deferred (binary `is_refunded` only). Destination: PR #84 (`5776a7b`).
- **2026-04-27** — Phase A → Phase B transition: ship the agent-kit `data-engineer` agent + LAW-skill catalog as v1; close the Strategic Reset; authorize Phase B build work. Destination: `docs/decisions/2026-04-27-phase-a-to-b-transition.md` (ADR) + PR #80.
- **2026-04-27** — Discovery Sprint inputs sharpened: "one wide mart per playbook chapter" architecture commitment + Owner-as-role column on `business-area-map.md`; mart-roadmap-rank scaffold-audit failure mode logged. Destination: PR #83 + WORKLOG entry for the cross-session learning.

## Open threads

- **Bridge extension (highest-leverage Phase B ticket).** `bridge_identity_contact_payment.sql:26` only sources `stg_stripe__charges` and tags `'stripe' as source_platform`. Add a `payments_unioned` CTE that UNION-ALL's `stg_fanbasis__transactions` (`fan.email` / `fan.phone` available; no separate billing fields) before tier matching. Until this lands, all Fanbasis revenue rows in `fct_payments` carry `bridge_status='unmatched'`. Invoke through `data-engineer` agent.
- **GHL trusted-copy decision** — single named blocker for several Tier B/refresh marts. Choose between legacy blob (1,314 conversation rows) and Phase-2 (101 rows). Resolves 92% undercount + four empty entities + dual-source ambiguity in one move. Independent of bridge ticket.
- **Other queued Phase B work:** period-grain rollups on existing wide marts; net-new facts (`fct_calls_held` from Fathom, `fct_opportunity_stage_transitions` from GHL, `fct_refunds`); `dim_typeform_form`; Tier C marts gated on vendor-support.
- **GCP IAM hygiene (cosmetic, not blocking).** `GCP_SA_KEY` in repo secrets carries `ingest@dee-data-ops.iam.gserviceaccount.com` (originally an ingestion SA, not a CI SA). The kit-purpose `sa-transform@project-41542e21-470f-4589-96d.iam.gserviceaccount.com` exists and is the right home for dbt-CI auth. After PR #84, three SAs hold `bigquery.user` + `dataEditor` on the consolidated project: `dbt-dev@dee-data-ops` (local dev), `ingest@dee-data-ops` (CI, current), `sa-transform@...` (kit-purpose, unused for CI yet). Future hygiene PR: rotate `GCP_SA_KEY` to `sa-transform@...` and update the workflow header comment to match reality.
- **Fathom → GHL contact join key** — attendee email reliability. Affects `fct_calls_held`.
- **GHL stage-transition event presence** — confirm raw landing carries stage-change events with timestamps before scaffolding `fct_opportunity_stage_transitions`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional. Lock after first production queries.
- **`bq-ingest` service repair**, **Typeform `form_id` gap**, **GHL `messages` / `users` / `tasks` 0-row upstream**, **Fathom transcript landing**, **Stripe staleness** — all paused per Strategic Reset; revisit on cutover resume.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look

- **Canonical roadmap (on main):** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` "Mart architecture commitment" + `.claude/rules/mart-naming.md` Rule 2
- **Phase B.1 artifacts (on main as of `5776a7b`):** `2-dbt/models/staging/fanbasis/stg_fanbasis__transactions.sql` + `_fanbasis__models.yml`; `2-dbt/models/warehouse/facts/fct_payments.sql` (Fanbasis CTE is now a real `ref()`)
- **Existing wide marts (auto-widen once bridge extension lands):** `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Bridge to extend (next ticket):** `2-dbt/models/warehouse/bridges/bridge_identity_contact_payment.sql`
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

- Last regen: 2026-04-28
- WORKLOG: skipped — Phase B.1 ship captured in PR #84 description + commit `5776a7b`; CI/IAM diagnostic captured in this state file's "GCP IAM hygiene" open thread; bridge follow-up captured in this state file's top open thread. No residual content needing append-only narration.
