# Mart collapse — consume `fct_calls_booked.{assigned_user_sk, pipeline_stage_sk}` from the fact

_Authored 2026-04-30. Branch: `Davv5/refactor/mart-consume-fct-sks`. Status: **PLAN ONLY — no SQL written.** Awaiting David review of the open questions in §7 before any implementation PR opens._

> **Headline recommendation.** Multi-PR migration (3 PRs) gated on a parity test, modeled on the F1/F2/F3 Speed-to-Lead pattern (`mart-naming.md` lessons-learned). PR-1 widens the fact with `selected_opportunity_id` (and a few opportunity outcome columns) so marts have a single, deterministic join axis to opps. PR-2 collapses `sales_activity_detail` and `lead_journey` to consume from the fact + the new join axis, behind a parity test. PR-3 deletes the divergent CTEs once parity has held green for one prod refresh cycle. **Blocking on David** for the two business-semantic questions in §7 before PR-1 merges.

## 1. Scope check — Y3 audit alignment

The Y3 marts retirement audit (`docs/plans/2026-04-29-y3-marts-audit.md`, branch `Davv5/feat/Y3-marts-audit`, PR #132) targets **`services/bq-ingest/sql/marts.sql`** (the legacy 6,064-line SQL writer that the `pipeline-marts-hourly` Cloud Run Job runs) and the `Marts.*` BigQuery dataset it produces. It does NOT target the dbt mart layer at `2-dbt/models/marts/` — that layer (`sales_activity_detail.sql`, `lead_journey.sql`, `speed_to_lead_detail.sql`, `revenue_detail.sql`) is the survival path. Y3 retires the legacy writer; this plan refactors the dbt-layer survivors. **No scope overlap, no scope conflict.** Proceed.

(Verified 2026-04-30: the Y3 verdict tables list zero `2-dbt/models/marts/*.sql` files.)

## 2. What the open thread actually said vs what the code shows

**Open-thread framing (project-state, post-PR #123):** "follow-up PR should collapse the marts to consume `fct_calls_booked.{assigned_user_sk, pipeline_stage_sk}` directly."

**What the code shows when you read it:**

- **`sales_activity_detail`** already reads `assigned_user_sk` + `pipeline_stage_sk` from the fact (lines 22–23, 246, 250). The framing is **satisfied** for those two columns. What the framing missed is the **`closer_and_outcome` CTE at lines 127–156**, which is doing two things the fact's SKs don't address:
  1. **Picking a different identity** — "closer" = `assigned_user_id` of the latest opp by `opportunity_created_at desc` (no time filter). This is **semantically not** "SDR at booking time"; it's "rep currently working the deal."
  2. **Sourcing outcome columns the fact does not expose** — `close_outcome`, `closed_at`, `lost_reason`, `last_stage_change_at`. These need opp-level data that lives in `stg_ghl__opportunities` and are not on the fact.

- **`lead_journey`** is **contact-grain**, not booking-grain. The fact lives at booking-grain. The mart's `latest_opportunity` CTE (lines 77–98) uses `opportunity_updated_at desc` (no time filter) — semantically "most-recently-touched opp per contact" = current pipeline state. Its `assigned_closer_name` (lines 136–145) joins through that latest-opp's `assigned_user_id` and filters to `dim_users.role = 'Closer'`. Different question, different grain, different filter.

**What the fact does not currently expose:** the `opportunity_id` of the picked opp. Marts cannot join back to opportunities "in lockstep with the fact's pick" without re-implementing the selection rule, which would re-create the divergence the PR #123 docstring set out to eliminate.

**Conclusion.** The "collapse" is genuinely partial today (the SK columns are wired in `sales_activity_detail`); the residual divergence is real and is about **opportunity-selection identity at booking time vs. current**, not about the SKs the framing named. The plan must address the residual, not just the SK consumption already done.

## 3. Business-semantic definitions — what we need David to confirm

The locked Speed-to-Lead headline metric (`CLAUDE.local.md`) is unambiguous on the numerator: "first outbound human SDR touch on the contact after booking, sourced from `raw_ghl.conversations` filtered by outbound direction, channel ∈ {CALL, SMS}, human-only via `lastManualMessageDate`, identity from `seeds/ghl_sdr_roster.csv`." That metric is **not** in question and is **not** at risk in this refactor — `sales_activity_detail.first_toucher_*` columns derive from `fct_outreach`, independent of `assigned_user_sk` and `closer_and_outcome`. The fact's docstring (lines 11–14) is explicit about this separation: the SKs are *diagnostic attribution*, not numerator inputs.

What IS in question are the **two diagnostic columns on the dashboard**: who is the "assigned closer" / "closer name" the dashboard shows alongside the booking? The two marts disagree today and the refactor must pick a definition.

### Question 3.1 — Closer identity in `sales_activity_detail`

In `sales_activity_detail`, what should `closer_name` mean?

| Definition | What it answers | Source under refactor |
|---|---|---|
| **(a) "Rep at booking time"** = `assigned_user_sk` from the fact. The SDR/closer GHL believed owned the contact at the moment Calendly fired. | "Who owned this booking when it landed?" | `fct_calls_booked.assigned_user_sk` (already wired). The current `closer_and_outcome.closer_name` collapses into the existing `au.assigned_user_name` and the CTE drops. |
| **(b) "Rep currently working the deal"** = `assigned_user_id` from the latest opp by `opportunity_updated_at desc` for the contact. | "Who's working this lead today?" | New "current opp" CTE (or join to a `dim_contact_current_opp` intermediate), distinct from the fact's booking-time opp. |
| **(c) Both columns**, distinguished. `assigned_user_*` from fact (booking-time); add `current_closer_*` for the latest-opp identity. | Both questions, no false economy. | Fact for (a); new CTE/intermediate for (b). |

CLAUDE.local.md doesn't specify. Code today is (b) — but the fact's PR #123 docstring (lines 17–22) calls the existing pattern "broken time filter" / divergent. **David must pick.** If unclear, default for the plan is **(c) both, distinguished** — adding a column is cheap, removing one later is cheap, but accidentally swapping (a) for (b) silently changes a dashboard number.

### Question 3.2 — `lead_journey` "current pipeline state"

`lead_journey.current_pipeline_name` / `current_stage_name` / `is_booked_stage` / `closed_won_flag` are derived from the latest opp by `opportunity_updated_at desc` (no time filter). This is semantically **"current state of this contact's most-recently-updated opportunity,"** which is **NOT** the same question as the fact's `pipeline_stage_sk` (which represents stage at booking time on a single booking). The two cannot collapse into one.

**Recommended treatment:** keep `lead_journey`'s `latest_opportunity` CTE. It is the right answer for the contact-grain "current state" question. Retire it only if David explicitly says "drop current pipeline state from Page 2 of the dashboard."

**This means `lead_journey` is largely out of scope for the SK-consumption refactor.** The only `lead_journey` change in scope is documenting that its `latest_opportunity` is intentionally distinct from the fact's "active opp at booking" — a one-comment-block edit, not a CTE rewrite.

### What David needs to confirm before PR-1

1. **Q3.1 disposition** — (a), (b), or (c)?
2. **Q3.2 confirmation** — is "current pipeline state" still wanted on Page 2? If yes, `lead_journey.latest_opportunity` stays as-is. If no, the CTE drops and so do those columns.

Pause point: do not merge PR-1 (the fact widening) until Q3.1 is answered, because the fact-shape change depends on it.

## 4. Fact-layer shape change

### 4.1 Recommended: add `selected_opportunity_id` to `fct_calls_booked`

The fact already does the selection (lines 99–116). It selects the row but does not project the `opportunity_id` of that row. Adding the column is one line (`opportunities.opportunity_id` in the `opportunity_at_booking` CTE projection + the SELECT in `final`).

**Why this matters:**

- **Single canonical join axis.** Any mart that wants to enrich a booking with opp-level data (`close_outcome`, `closed_at`, `lost_reason`, `last_stage_change_at`) gets exactly one rule for which opp: "the one the fact picked." No `QUALIFY ROW_NUMBER()` in the mart layer (that's the data-modeling-process rule's Maxim 2 — `QUALIFY = 1` in a downstream model is a code smell signaling upstream grain is wrong). The fact already partitions on `event_id`; the mart joins on `selected_opportunity_id`.
- **No fan-out risk.** `opportunity_id` is the PK of `stg_ghl__opportunities` (verify before PR-1 — `data-modeling-process.md` Maxim 4). Mart joins to `stg_ghl__opportunities` on that key are 1:1.
- **Costs nothing.** One column on a small fact table. The selection CTE already computes the row.

### 4.2 Optional: add outcome columns to the fact directly

Two paths to surface `close_outcome` / `closed_at` / `lost_reason` / `last_stage_change_at` to `sales_activity_detail`:

| Path | Where the columns live | Pros | Cons |
|---|---|---|---|
| **Path X — fact widens.** Add `close_outcome`, `closed_at`, `lost_reason_id`, `last_stage_change_at` to `fct_calls_booked` directly, sourced from `stg_ghl__opportunities` joined on the new `selected_opportunity_id`. | On the fact. | Marts join once. Column semantics are pinned at the fact's grain (booking × selected-opp). The `warehouse.md` rule "facts contain numeric aggregables + event-grain timestamps" allows event-grain timestamps; `closed_at` / `last_stage_change_at` qualify. | `lost_reason_id` is a categorical FK, not a numeric / timestamp. `warehouse.md` says "no descriptive text in facts" — `lost_reason_id` is the FK form, not the descriptive name, so this is borderline-acceptable but worth noting. |
| **Path Y — mart joins through `selected_opportunity_id`.** Fact only adds `selected_opportunity_id`; `sales_activity_detail` joins to `stg_ghl__opportunities` (or to a new `dim_opportunities` if we ever build one) and reads outcome columns there. | In the mart. | Keeps the fact narrow per `warehouse.md` ("facts = SK + FK SKs + numeric aggregables + event-grain timestamps"). Outcome data stays in its source layer; the mart picks what it needs. | Two joins instead of one (fact → opportunities → users). Slight repetition if multiple marts ever want outcome columns. |

**Recommended: Path Y.** Fewer fact-shape changes; keeps the fact strictly Kimball; outcome columns are descriptive opportunity attributes that don't earn a place on the fact yet (only one mart consumes them today). If a second consumer emerges, revisit.

**Net fact change in PR-1:** one new column, `selected_opportunity_id`. No outcome columns on the fact.

## 5. Mart shapes after refactor

### 5.1 `sales_activity_detail`

**CTEs that drop:**

- `closer_and_outcome` (lines 127–156). Replaced by:
  - `au.assigned_user_name` / `au.assigned_user_role` already wired via the existing `assigned` CTE on `b.assigned_user_sk` for the **booking-time identity** (Q3.1 disposition (a) or (c)).
  - A new `outcomes` CTE that joins `stg_ghl__opportunities` on `b.selected_opportunity_id = opp.opportunity_id` for `close_outcome` / `closed_at` / `lost_reason` / `last_stage_change_at`. **Single join key, no `QUALIFY`, no fan-out** (assuming opp PK uniqueness check passes — Maxim 4).

**CTEs that stay:**

- `fct_bookings`, `contacts`, `users`, `outreach`, `stages`, `assigned`, `first_touch`, `first_toucher`, `had_activity_1hr` — all unrelated to opp selection. Untouched.
- The Speed-to-Lead numerator (`first_touch` / `first_toucher` / `is_within_5_min_sla` / `had_any_sdr_activity_within_1_hr`) is not affected by this refactor. Parity test must prove this.

**Columns under Q3.1 disposition (a) — "rep at booking":**
- `closer_name` and `closer_role` columns become aliases or are renamed to `assigned_user_name` / `assigned_user_role` (which are already in `final`). Net: lose two columns, save one CTE, dashboards repoint.

**Columns under Q3.1 disposition (c) — "both, distinguished":**
- `assigned_user_name` / `assigned_user_role` (booking-time) — already exist, unchanged.
- New `current_closer_name` / `current_closer_role` — sourced from a new `current_opp` CTE that picks the latest opp per contact by `opportunity_updated_at desc` and joins `dim_users` on `assigned_user_id`. This is the same pattern as `lead_journey.assigned_closer` — option to extract to an intermediate model `int_contact_current_closer` if both marts use it (Step 4 physicalization decision per `data-modeling-process.md`).
- `closer_name` / `closer_role` (the existing columns) decision: keep aliased to whichever of (a) or (b) we pick as "primary," OR rename to remove ambiguity. Recommend renaming both to be explicit: drop the bare `closer_name`.

### 5.2 `lead_journey`

**No structural change recommended.** This mart's `latest_opportunity` and `assigned_closer` CTEs answer a contact-grain "current state" question that the booking-grain fact does not address. The fact's SKs are not the right inputs for this mart.

**One edit in scope:** add a header docstring comment block (line 1+) clarifying that `latest_opportunity` is intentionally **NOT** the same as `fct_calls_booked.{assigned_user_sk, pipeline_stage_sk}`:

```
-- latest_opportunity is contact-grain "most-recently-updated opp"
-- = current pipeline state. This is intentionally distinct from
-- fct_calls_booked.{assigned_user_sk, pipeline_stage_sk}, which is
-- "active opp at the moment of booking" (see fct_calls_booked.sql
-- header). Both questions are valid; do not collapse them.
```

This is the smallest possible nudge that prevents a future-Claude from "helpfully" rewriting `lead_journey` to consume the fact's SKs and silently changing Page 2 of the dashboard.

**If David answers Q3.2 with "drop current pipeline state":** the `latest_opportunity`, `assigned_closer`, and 6+ derived columns drop. That is a dashboard-shape change, not a refactor — out of this plan's scope; spin a separate doc.

### 5.3 Other marts

`speed_to_lead_detail.sql` and `revenue_detail.sql` already consume `fct_calls_booked.pipeline_stage_sk` cleanly (verified in `speed_to_lead_detail`; `revenue_detail` is payment-grain and unaffected). No changes.

## 6. Migration shape — multi-PR, parity-gated

Modeled on the F1/F2/F3 Speed-to-Lead refactor (`mart-naming.md` lessons-learned, 2026-04-22 — the only precedent in this repo for "fact-first then collapse marts" with a parity gate).

### PR-1 — Widen the fact (additive only)

**Changes:**
- Add `opportunities.opportunity_id` to the `opportunity_at_booking` CTE projection.
- Project `opportunity_at_booking.opportunity_id as selected_opportunity_id` in `final`.
- Update `_facts__models.yml` with the new column + `not_null` is **NOT** added (NULL when no pre-booking opp exists, by design — same pattern as `assigned_user_sk`). Add `relationships` test to `stg_ghl__opportunities.opportunity_id`.
- Update `_facts__docs.md` description.
- Pre-merge gate: **`altimate-sql-review` on the changed `fct_calls_booked.sql`** (LAW 1, deterministically hooked).

**Acceptance:**
- `dbt build --select fct_calls_booked` green in CI.
- Six-perspective DQ check (`warehouse.md`): unique on `selected_opportunity_id` is **NOT** asserted (multiple bookings can share an opp); referential integrity test added.
- Row count of `fct_calls_booked` must not change. `dbt run` + `dbt test` clean.

**Why additive-only:** PR-1 introduces no behavior change for any consumer. Marts continue to read what they read today. Safe to merge alone.

### PR-2 — Collapse `sales_activity_detail` + add parity test

**Changes:**
- Refactor `sales_activity_detail.sql`: drop `closer_and_outcome` CTE, add `outcomes` CTE keyed on `selected_opportunity_id`, rewire columns per Q3.1 disposition.
- (If Q3.1 = (c)) add a new `current_opp` CTE OR scaffold `int_contact_current_closer` intermediate (Step 4 physicalization call: if `lead_journey` also gains the same logic, intermediate; if only `sales_activity_detail` uses it, inline).
- Edit `lead_journey.sql` header docstring per §5.2.
- **Add a singular dbt test** `sales_activity_parity` modeled on F1/F2/F3's `stl_headline_parity`:
  - The test compares the post-refactor `sales_activity_detail` against a frozen-snapshot pre-refactor copy (built from `main` once) on:
    1. **Row count** must match exactly.
    2. **`is_within_5_min_sla` distribution** must match exactly (this is the locked headline metric — non-negotiable).
    3. **`first_outbound_touch_at` per booking_id** must match exactly.
    4. **`assigned_user_name` per booking_id** must match exactly under Q3.1 (a) or (c); under (b), pre-refactor's `closer_name` corresponds to post-refactor's `current_closer_name`.
  - Tolerance for `close_outcome` / `closed_at` / `lost_reason`: **document deltas and require David sign-off** rather than assert byte-equality. The pre-refactor `closer_and_outcome` selection rule (latest opp by `opportunity_created_at desc`, no time filter) and the post-refactor selection (`selected_opportunity_id` from the fact = `<= booked_at` boundary) ARE different rules. Outcome-column drift is expected and is the whole point of unifying on the fact's rule. Capture the diff in the PR description.
- Pre-merge gates: **`altimate-sql-review` on `sales_activity_detail.sql`** (LAW 1). **`altimate-data-parity` is NOT the right tool** here because the contract is "preserve headline-metric numerator, document outcome-column drift" — not "byte-identical." A custom dbt singular test is the precedent (F1/F2/F3's `stl_headline_parity`).

**Acceptance:**
- Parity test green in CI on the PR.
- Numerator-distribution diff in the PR description shows zero drift on `is_within_5_min_sla` and `first_outbound_touch_at` per booking.
- Outcome-column diff documented; David approves the delta as expected.
- Headline metric on the dashboard does not move (read pre and post in the BI surface; numbers identical).

**Coexistence window:** PR-2 ships the new mart shape. Old `closer_and_outcome` CTE is gone. The parity test stays green for one full prod refresh cycle (≥ 24h) before PR-3 lands.

### PR-3 — Retire the parity test + close the open thread

**Changes:**
- Delete the `sales_activity_parity` singular test (it has served its purpose; keeping it forever fails on legitimate future changes to `sales_activity_detail`).
- Update `.claude/state/project-state.md` Open Threads to mark the post-PR-#123 follow-up done.
- Update the `fct_calls_booked.sql` docstring at line 22–25 — the "follow-up PR will collapse the marts" sentence is now historical; update to "marts now consume `selected_opportunity_id` directly (PRs #N, #N+1)."

**Why a separate retirement PR:** F1/F2/F3 precedent. Retiring the parity test in the same PR that introduces it gives no live coverage window; the test only earns its keep by running in CI for at least one refresh cycle on the new shape.

### Why not a single PR?

- **Parity test needs a baseline.** A single-PR shape requires the test to compare against a snapshot — possible (build the snapshot inside the PR's CI job from a freshly-checked-out `main`), but operationally fragile in this repo's CI shape.
- **Risk concentration.** A single PR ships the fact change, the mart rewrite, and removes the parity test all at once. If anything goes wrong, the rollback unwinds the fact change too.
- **Coexistence window is the safety.** F1/F2/F3 lessons-learned: "ship the new model alongside the old as F1/F2, let them coexist for at least one prod refresh cycle with the parity test green, then deprecate in F3 (this track)." The 3-PR shape is exactly that pattern adapted to a refactor of an existing single mart.

## 7. Risk register

### Top three risks

1. **Locked headline metric drift on `is_within_5_min_sla`.** This is THE highest-stakes failure mode. The numerator is sourced from `fct_outreach` + `dim_users` and is independent of the SKs / `closer_and_outcome` CTE. Refactor SHOULD not touch it. Defense: parity test asserts byte-identical `is_within_5_min_sla` per booking pre-and-post. Rollback: revert PR-2.
2. **Q3.1 ambiguity → wrong `closer_name` semantics ship.** If we guess wrong on (a) vs (b) vs (c), the dashboard's "closer" column silently changes meaning. The mart-naming.md "fewer wider marts" maxim does not protect against this — it's a column-semantics error inside one mart. Defense: David answers Q3.1 in writing before PR-1 merges. Recommended default if David is unreachable: (c) both columns, distinguished — strictly more information, can shrink later.
3. **Outcome-column drift surprises a downstream BI surface.** Post-refactor `close_outcome` / `closed_at` / `lost_reason` come from the fact-selected opp (active at booking) instead of the latest opp on the contact. For bookings whose contact subsequently got a NEW opp (re-engagement, additional pipeline), the outcome column will show the OLD opp's outcome — possibly stale. Defense: PR-2 description must enumerate the row-count of bookings where `selected_opportunity_id` differs from the latest opp on the contact (this is the affected slice). David approves before merge.

### Other risks

4. **`opportunity_id` PK uniqueness assumption.** Plan assumes `stg_ghl__opportunities.opportunity_id` is unique. Verify before PR-1 (Maxim 4 — `COUNT(1)` after every join in the new `outcomes` CTE; if not unique, fan-out and the parity test will catch it).
5. **`selected_opportunity_id` NULL handling.** Bookings with no pre-existing opp (~unknown %) get NULL `selected_opportunity_id`. The new `outcomes` CTE join must be `LEFT JOIN`, and downstream columns must `COALESCE` or accept NULL. Same pattern as the existing NULL `assigned_user_sk` rows. Tested by the parity test.
6. **Parity test scope creep.** Tempting to assert byte-equality on every column. Resist — the WHOLE POINT of the refactor is that some columns intentionally change rule (outcome columns). The parity test asserts only what MUST not move; the diff narrative captures what intentionally changes.
7. **`lead_journey` future drift.** Adding the comment block is the only defense against a future agent collapsing `latest_opportunity` to consume the fact. Stronger defense (and additive): a unit test asserting `lead_journey.current_pipeline_name` does NOT round-trip through `fct_calls_booked`. Defer to PR-2 if cheap; skip if expensive.

### Where the refactor could silently change a number

| Surface | Number | Risk under refactor |
|---|---|---|
| Dashboard Speed-to-Lead headline tile | `is_within_5_min_sla` count | **Zero** — sourced from `fct_outreach` first-touch, untouched. Parity test asserts. |
| Dashboard SDR leaderboard | `first_toucher_name` aggregations | **Zero** — sourced from `fct_outreach` + `dim_users`, untouched. Parity test asserts. |
| Dashboard "closer" column | `closer_name` semantics | **Material risk** under Q3.1 disposition (a) or (b). Hence the question must resolve before PR-1. |
| Dashboard outcome panels | `close_outcome` / `lost_reason` distributions | **Material risk by design** — refactor unifies on a different selection rule. PR-2 description enumerates affected rows; David approves. |
| `lead_journey` Page 2 | `current_pipeline_name` / `current_stage_name` / `assigned_closer_name` | **Zero** if Q3.2 = "keep" (no structural change). Material if Q3.2 = "drop" (out of scope, separate plan). |

### Rollback path

- **PR-1 rollback:** revert the fact-widening commit. No mart depends on `selected_opportunity_id` until PR-2, so PR-1 is safe to revert independently.
- **PR-2 rollback:** revert the mart change + delete the parity test. PR-1's `selected_opportunity_id` column remains harmlessly on the fact.
- **PR-3 rollback:** N/A — only deletes a passing test. If we need the parity test back, restore from git.

## 8. Open questions for David (must resolve before PR-1)

1. **Q3.1 — `sales_activity_detail.closer_name` semantics.** (a) booking-time, (b) current, (c) both distinguished?
2. **Q3.2 — `lead_journey` "current pipeline state" — keep on Page 2?**
3. **Outcome-column drift acceptance.** Are you OK with `close_outcome` / `closed_at` / `lost_reason` shifting from "latest opp" to "active opp at booking time" (the fact's rule)? This is the rule-unification PR #123 set up; the question is just whether you want to accept the drift now or punt the outcome-column portion to a follow-up PR.

## 9. Where this lives

- This plan: `docs/plans/2026-04-30-mart-collapse-fct-sks-plan.md`
- Branch: `Davv5/refactor/mart-consume-fct-sks`
- Triggers: post-PR-#123 open thread in `.claude/state/project-state.md`; `fct_calls_booked.sql` lines 22–25 docstring promise.
- Adjacent: Y3 marts retirement audit (`docs/plans/2026-04-29-y3-marts-audit.md`) — different layer, no conflict.

## 10. Skills / tooling that will fire on implementation

- **PR-1 (fact widen):** `altimate-sql-review` on the `fct_calls_booked.sql` diff (LAW 1, hooked). `altimate-schema-migration` if the operator considers any of the column changes a DDL migration (additive column on a table-materialized model — borderline; default to running it if in doubt — LAW 3 doesn't allow "this case is small" exemptions).
- **PR-2 (mart collapse):** `altimate-sql-review` on `sales_activity_detail.sql` (LAW 1, hooked). NOT `altimate-data-parity` — the contract is custom (numerator parity + outcome-column drift accepted), so a hand-written dbt singular test (per F1/F2/F3) is the right tool. CE adversarial reviewer pass on the diff before merge per `use-data-engineer-agent.md` "Reviews always pair" — high-stakes, headline-metric-adjacent, large surface.
- **PR-3 (retire parity test):** `altimate-sql-review` on the changed mart YAML; routine.
