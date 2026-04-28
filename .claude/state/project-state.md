# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-28 (post-PR #95: local-CI wrapper + ADC profile targets shipped after Phase B.4)_

## Where we are

- **Phase B (Layer Build) is live.** Five feature tickets shipped today:
  - PR #84 (`5776a7b`) — `stg_fanbasis__transactions` view + Stripe+Fanbasis union (Fanbasis: 466 rows / $170,712 gross / 9 refunds, Apr 2025 → Apr 2026)
  - PR #86 (`9db0899`) — `bridge_identity_contact_payment` extended UNION + composite PK `(source_platform, payment_id)` + `tier_unmatched` refactored as anti-join (no payment can be silently dropped); 5 new bridge tests. **Match rates: Stripe 94.22%, Fanbasis 99.36%** in dev_david
  - PR #88 (`1f5c73b`) — `fct_revenue` → `fct_payments` semantic rename
  - PR #90 (`ac38fb6`) — `stg_fanbasis__refunds` event-grain staging + `fct_refunds` Fanbasis-only warehouse fact + `fanbasis_refund_parity` singular test. **9 refund events / $2,500 USD, 100% bridge-attributed** (8 email_exact + 1 billing_email_direct)
  - PR #92 (`aae6f29`) — `revenue_detail` net-of-refunds extension: 4 new columns (`refunds_total_amount`, `refunds_total_amount_net`, `refunds_count`, `net_amount_after_refunds` with `CASE` branching by `source_platform` to defend against future Stripe arm) + `revenue_detail_refunds_parity` singular test. Verified locally: Fanbasis $162,445.73 → $159,945.73 net-of-refunds (delta $2,500 ✓); Stripe `sum_net_after_refunds == sum_net` (CASE branch returns net_amount).
- **Existing wide marts auto-widened post-bridge:** `lead_journey` revenue rollups now include Fanbasis on next refresh; `sales_activity_detail` and `speed_to_lead_detail` are revenue-independent and unchanged.
- **Local dev-loop tooling shipped (PRs #94 + #95).** New `dev_local` / `ci_local` profile targets (ADC method, no SA keyfile required) + `2-dbt/scripts/local-ci.sh` wrapper that mirrors the GH `dbt-ci.yml` workflow. `.env` defaults to `DBT_TARGET=dev_local`; switch to `dev` when a consolidated-project SA key is provisioned. GH Actions remains the merge gate — local CI is for the developer loop and emergency bypass.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path; raw `notebook_query` calls are reserved for ad-hoc lookups.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 15 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` U4a+ resumes when GHL trusted-copy decision lands.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **Sprint (closed):** `docs/plans/2026-04-24-strategic-reset.md` — Phase A complete; Phase B build is live.
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-28** — Phase B.4 ships: `revenue_detail` extended with net-of-refunds columns. `net_amount_after_refunds` branches `CASE WHEN source_platform = 'stripe' THEN net_amount ELSE net_amount - refunds_total_amount END` because Stripe's net is already net of refunds at staging (`amount_captured_minor - amount_refunded_minor`) while Fanbasis's is net of fees only. Mechanical branching beats doc-only future-Claude warnings; parity test gates per source_platform regardless. Destination: PR #92 (`aae6f29`).
- **2026-04-28** — Phase B.3 ships: `fct_refunds` is Fanbasis-only at event grain. Stripe arm omitted because `stg_stripe__charges.amount_refunded_minor` is at charge-grain (would lose temporal fidelity); Stripe is also banned at D-DEE so the asymmetry matches reality. Contact attribution via the bridge joined on the parent payment's `(source_platform, payment_id)` — refunds inherit the parent's contact, not re-matched. Destination: PR #90 (`ac38fb6`).
- **2026-04-28** — Local-CI tooling lands. New `dev_local` / `ci_local` profile targets use `method: oauth` (ADC) so a developer can run dbt without an SA keyfile; the `2-dbt/scripts/local-ci.sh` wrapper mirrors GH `dbt-ci.yml` (provision per-PR dataset → `dbt build --target {ci|ci_local}`). Driven by today's GH Actions `degraded_performance` incident — David pushed back on a passive "wait it out" recommendation, prompting the bypass + then the codified pattern. GH remains the merge gate; local CI is dev-loop + emergency bypass. Destinations: PR #94 (`69804be`) + #95 (`611b5f0`). Cross-session pattern: `feedback_local_ci_bypass.md` memory.

## Open threads

- **Next Phase B candidates** (pick from these, none currently in flight):
  - **`fct_opportunity_stage_transitions`** — first confirm raw landing carries stage-change events with timestamps. Blocker class: `dbt-staging` audit, then warehouse-fct-scaffold.
  - **`dim_typeform_form`** — blocked on Typeform `form_id` gap (every response has NULL `form_id` upstream — U9 extractor work).
  - **Period-grain rollups** — `revenue_detail_by_week`, `lead_journey_by_month`, etc. on top of existing wide marts. Premature at current scale.
- **Float64-for-money tech debt (Fanbasis)** — `stg_fanbasis__transactions` and `stg_fanbasis__refunds` cast amount fields to `float64`; should be `numeric` to avoid FP rounding drift on aggregation. PR #92's parity test uses $0.01 tolerance to absorb this; cleanup PR when refund volume justifies.
- **GHL trusted-copy decision** — single named blocker for several Tier B/refresh marts. Choose between legacy blob (1,314 conversation rows) and Phase-2 (101 rows). Resolves 92% undercount + four empty entities + dual-source ambiguity in one move.
- **GCP IAM hygiene (cosmetic, not blocking).** `GCP_SA_KEY` in repo secrets carries `ingest@dee-data-ops.iam.gserviceaccount.com`; the kit-purpose `sa-transform@project-41542e21-470f-4589-96d.iam.gserviceaccount.com` is the right home for dbt-CI auth. Local-equivalent SA key for the consolidated project is also unprovisioned; until it lands, ADC via `dev_local` / `ci_local` is the working path (this is no longer a blocker).
- **Fathom → GHL contact join key** — attendee email reliability. Affects future `fct_calls_held`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional. Lock after first production queries.
- **`bq-ingest` service repair**, **Typeform `form_id` gap**, **GHL `messages` / `users` / `tasks` 0-row upstream**, **Fathom transcript landing**, **Stripe staleness** — all paused per Strategic Reset; revisit on cutover resume.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look

- **Canonical roadmap (on main):** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` "Mart architecture commitment" + `.claude/rules/mart-naming.md` Rule 2
- **Phase B artifacts (on main):** `2-dbt/models/staging/fanbasis/{stg_fanbasis__transactions,stg_fanbasis__refunds}.sql` + `_fanbasis__models.yml`; `2-dbt/models/warehouse/facts/{fct_payments,fct_refunds}.sql`; `2-dbt/models/warehouse/bridges/bridge_identity_contact_payment.sql` + `_bridges__{models.yml,docs.md}`; `2-dbt/models/marts/revenue_detail.sql` + `_marts__{models.yml,docs.md}`; `2-dbt/tests/{bridge_payment_count_parity,bridge_match_rate_floor,fanbasis_refund_parity,revenue_detail_refunds_parity}.sql`
- **Existing wide marts (auto-widened post-bridge):** `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Local dev loop:** `2-dbt/scripts/local-ci.sh` (wrapper mirroring GH `dbt-ci.yml`); `2-dbt/profiles.yml` (`dev_local` / `ci_local` ADC targets); `2-dbt/README.md` "Local CI" section; cross-session pattern at memory `feedback_local_ci_bypass.md`.
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

- Last regen: 2026-04-28 (post-PR #95 merge)
- WORKLOG: skipped — PR #94 (local CI tooling) and #95 (review follow-ups) are tooling-only, fully captured in PR descriptions + commit messages. The cross-session operational pattern (when to bypass GH Actions with local CI) lives in `feedback_local_ci_bypass.md` memory. The "Local dev env stale" thread is now retired (workaround became the default path); the GCP IAM hygiene thread absorbs the residual SA-key-for-consolidated-project follow-up. No residual content needing WORKLOG entry.
