# Session Handover — Track M: `revenue_detail` payment-grain mart scaffolded (Track-E-blocked)

**Branch:** `Davv5/Track-M-revenue_detail-mart-payment-grain-unmatched-revenue-transparency`
**Timestamp:** `2026-04-20_12-46` (local)
**Author:** Claude Code (Opus 4.7), auto mode
**PR:** not yet opened — will open immediately after this handover

---

## Session goal

Execute Track M per `/Users/david/Documents/data ops/docs/worktree-prompts/track-m.md`: ship the payment-grain `revenue_detail` mart with identity/campaign/closer attribution and an unmatched-revenue transparency surface, plus a singular release-gate test asserting revenue / row count / unmatched-share invariants against the 2026-03-19 oracle.

## Changed files

```
dbt/models/marts/revenue_detail.sql             — created — payment-grain mart, unmatched rows retained
dbt/models/marts/_marts__models.yml             — created — 18-col contract; enums tested via accepted_values
dbt/models/marts/_marts__docs.md                — created — doc block w/ DQ semantics + Fanbasis caveat
dbt/tests/release_gate_revenue_detail.sql       — created — 3-assertion singular test vs oracle seed
WORKLOG.md                                      — edited   — appended 2026-04-20 Track M entry
docs/handovers/Davv5-Track-M-...-2026-04-20_12-46.md — created — this file
```

## Commands run / run IDs

- `python3 -m pip install --user sqlfluff` (version 4.1.0)
- `python3 -m sqlfluff lint dbt/models/marts/revenue_detail.sql dbt/tests/release_gate_revenue_detail.sql` — remaining diagnostics are LT01 multi-space-before-`as` (matches every merged staging model) and one LT02 `where`-on-same-line that matches the `dbt_style_guide.md` example
- `python3 -c "import yaml; yaml.safe_load(open('dbt/models/marts/_marts__models.yml'))"` — YAML OK
- **Not run:** `dbt build --target dev --select revenue_detail` and `dbt test --target dev --select revenue_detail release_gate_revenue_detail` — Track E refs (`fct_revenue`, `bridge_identity_contact_payment`, `dim_contacts`, `dim_users`) do not exist on `main`; compile cannot resolve. Also no `.venv` / `.env` / `dbt` binary in this Orca worktree yet. Verification parity query in the track prompt (`bq query ... dev_david.revenue_detail`) deferred to after Track E merges

## Decisions made

- **Kept unmatched rows in the mart** because Page 3 of the dashboard ships an unmatched-revenue transparency tile as a deliberate trust signal; filtering would understate revenue and defeat the whole point of the track
- **Closer = latest GHL opportunity with Closer-role assignee**, resolved via `qualify row_number() over (partition by contact_id order by opportunity_updated_at desc) = 1` on `stg_ghl__opportunities` joined to `dim_users` filtered to `role = 'Closer'`
- **Corrected column reference** from the track-prompt draft's `opp.updated_at` to the real `opportunity_updated_at` alias exposed by `stg_ghl__opportunities.sql:44`
- **Left-table-first on join ON clauses** per `dbt_style_guide.md:236-239`; the draft in the track prompt had the ON expressions reversed, which sqlfluff ST09 would have flagged
- **Deferred actual `dbt build` + BQ parity check to next session** because Track E is the merge gate

## Unresolved risks

- [ ] **Track E not merged** — `revenue_detail.sql` refs `fct_revenue`, `bridge_identity_contact_payment`, `dim_contacts`, `dim_users`; until Track E lands on `main`, `dbt parse` / `dbt build` on this branch will fail. Mart SQL was written against the contract implied by the track prompt + `v1_build_plan.md`; any column-name drift in the real Track E models will require a small rename pass. Owner: whoever ships Track E
- [ ] **Fanbasis parity gap** — oracle total is $356,935; if Fanbasis revenue is included there and the v1 `fct_revenue` is Stripe-only (per CLAUDE.local.md Week-0 credentials blocker), the release-gate test's 5% revenue tolerance will fire. Handling per track prompt: widen tolerance with a Fanbasis link, do **not** filter rows. Owner: David (Fanbasis API creds)
- [ ] **`_marts__models.yml` merge coordination** — Tracks F (`sales_activity_detail`) and L (`lead_journey`) also extend this file in parallel worktrees. Whoever merges second rebases and unions the `models:` list. No schema conflict expected; entries are independent
- [ ] **Residual sqlfluff diagnostics** — LT01 multi-space-before-`as` (project-wide convention, not a new issue); one LT02 `where`-on-same-line at `dbt/tests/release_gate_revenue_detail.sql:55`. Neither blocks CI based on existing merged-file precedent

## First task for next session

**Rebase this branch onto `main` once Track E is merged, then run `dbt build --target dev --select +revenue_detail` followed by `dbt test --target dev --select revenue_detail release_gate_revenue_detail`, capture the actual row count / total revenue / unmatched-share numbers, append them to the Track M worklog entry, and reconcile any column-name drift between the mart SQL and the landed Track E models.**

## Context links

- Track prompt: `/Users/david/Documents/data ops/docs/worktree-prompts/track-m.md`
- Scope: `client_v1_scope_speed_to_lead.md`, `v1_build_plan.md` §Phase 3 (facts + marts)
- Mart-naming rule applied: `.claude/rules/mart-naming.md` (business-friendly name, no `fct_` prefix, wide table over many narrow reports)
- Worklog entry: `WORKLOG.md` §2026-04-20 Track M
- Oracle seed: `dbt/seeds/validation/oracle_dashboard_metrics_20260319.csv` — `Total Revenue (USD)` row supplies the parity target
- Related tracks: Track E (dependency, warehouse facts + bridge), Track F (`sales_activity_detail`, parallel mart), Track L (`lead_journey`, parallel mart)
