# Gap Analysis — Raw vs Staging Delta

**Sprint supporting artifact.** Delta-centric view. Answers: "what should exist that doesn't, and what does that block?"

Last regenerated: 2026-04-24. Pairs with `source-inventory.md` (source-centric) and `staging-models.md` (model-centric).

Every gap below is classified as:
- **Phase A** — addressable within the Discovery Sprint (docs, scoping decisions, small dbt-only work that survives roadmap churn)
- **Phase B** — deferred until after Gold-layer roadmap locks
- **Out of scope** — not a Merge-repo fix

Reversibility test applied to every "Phase A" classification: *would this work survive a roadmap change in two weeks?* Yes for all Phase A items listed — they're either factual audits or additive unblockers.

---

## Gap taxonomy — the five kinds

1. **Source-level gap** — a whole data source has no staging footprint despite raw data landing
2. **Entity-level gap** — a raw entity is landing but no staging model reads it
3. **Hollow-model gap** — a staging model exists but the raw table it reads is empty
4. **Column-level gap** — a staging model is live but a key column is NULL because the extractor doesn't write it
5. **Pipeline-health gap** — staging is fine but an upstream writer is broken, so staging data is stale

---

## Gap 1 — Fanbasis has zero staging despite live raw (source-level)

**What exists:** `Raw.fanbasis_transactions_txn_raw` — 455 rows, fresh daily at 07:27 UTC. Growing ~20 txn/day.

**What's missing:**
- No `stg_fanbasis__*.sql` files anywhere under `2-dbt/models/staging/fanbasis/`.
- No `_fanbasis__models.yml`.
- `_fanbasis__sources.yml` is a placeholder with `tables: []` and still points at pre-U2 project `dee-data-ops`.

**Why it matters:** Per memory `project_stripe_historical_only.md`, Fanbasis is D-DEE's **live revenue source** (Stripe is banned / historical-only). Every revenue-area question — ARR, churn, refunds, cohort retention — routes to Fanbasis data that is currently invisible to dbt. This is the single highest-leverage unmodelled source in the stack.

**Root cause:** API docs and credentials were owed by D-DEE (Week-0 ask). Raw landing started quietly; dbt-side never followed.

**Disposition:**
- **Phase A work (sprint scope):** (a) retarget `_fanbasis__sources.yml` to `project-41542e21-470f-4589-96d`; (b) audit the raw payload shape via BQ query (10 min); (c) add one exploratory `stg_fanbasis__transactions` view so the raw is queryable from dbt. All three survive roadmap churn — they make the data visible without committing to downstream marts.
- **Phase B work (post-roadmap):** revenue/churn/refunds marts using the new staging model.
- **Out of scope:** extractor hardening, additional Fanbasis entities (refunds, chargebacks), authoritative Fanbasis API doc review — blocked on D-DEE Week-0 ask.

**Priority:** P0 — highest-leverage unblocker available today.

---

## Gap 2 — GHL hollow + missing entity models (entity-level + hollow-model)

**What exists:**
- `stg_ghl__messages` — 0 rows upstream, compiles empty.
- `stg_ghl__users` — 0 rows upstream, compiles empty.
- Raw tables for `ghl__notes_raw` and `ghl__tasks_raw` exist with 0 rows, **no staging models**.

**What's missing / broken:**
- Four GHL entity types (messages, notes, tasks, users) never populated upstream on the Phase-2 extractor path.
- Legacy blob (`Raw.ghl_objects_raw`) has 68,314 rows spanning broader entity coverage but is 3 days stale.
- Phase-2 `conversations` is 92% undercounted (101 vs 1,314 in legacy blob).

**Why it matters:** GHL is the richest business-signal source (contacts, opportunities, conversations, pipeline stages, SDR activity). Current Phase-2 path delivers a crippled view. Any business area touching SDR activity, AE conversations, task completion, or admin roster depends on either fixing Phase-2 or fully consolidating on the legacy blob.

**Disposition:**
- **Phase A work:** (a) decide **Phase-2 vs legacy-blob as authoritative** — this decision alone resolves the 92% undercount, the 4 empty entities, and the dual-source confusion; (b) document the decision in `gold-layer-roadmap.md` before any new mart work; (c) if legacy-blob-authoritative, scope the shim work (mirror Calendly/Stripe/Typeform/Fathom pattern). Audit-only, docs-only. Survives any roadmap change.
- **Phase B work:** actually repoint the GHL staging models at the legacy blob (if that's the decision). Or fix `bq-ingest` and reconcile Phase-2.
- **Out of scope:** `bq-ingest` service repair (GTM repo), upstream GHL extractor changes to populate `messages` / `notes` / `tasks` / `users`.

**Priority:** P0 — blocks roadmap clarity for every GHL-dependent business area.

---

## Gap 3 — Fathom transcripts absent (column-level)

**What exists:** 1,157 Fathom calls in `Raw.fathom_calls_raw`, fresh hourly. Top-level columns (`is_revenue_relevant`, classification fields) populated.

**What's missing:** `payload_json.$.transcript` is absent across 100% of rows. The `Raw.fathom_call_intelligence` LLM-analysis table is present but 0 rows (analysis pipeline dead since 2026-04-03).

**Why it matters:** Call-content questions (sentiment, objection handling, closing quality, keyword prevalence) are blocked. Metadata-only questions (call volume, classification, revenue-relevance) work today.

**Disposition:**
- **Phase A work:** (a) document root-cause hypothesis per `source-inventory.md` §5 (extractor calls `/meetings` but not `/transcripts`, OR async transcript job not polled, OR auth scope gap); (b) scope whether the sprint's business-area-map surfaces a real need for transcripts in the Gold layer.
- **Phase B work (U6 scope):** fix the transcript landing if the roadmap justifies it.
- **Out of scope:** LLM analysis pipeline revival — separate architectural decision scoped to U6.

**Priority:** P1 — depends on roadmap outcome. If zero Gold marts need transcript content, transcript fix drops further.

---

## Gap 4 — Typeform `form_id` NULL + `forms` unmodelled (column-level + entity-level)

**What exists:**
- `stg_typeform__responses` — fresh hourly, 5,085 rows.
- `Raw.typeform_objects_raw` contains both `responses` and `forms` entity_types in one blob.

**What's missing:**
- `form_id` is NULL in staging (extractor never wrote it). `not_null` test was lifted in U3 with "restore at U9" note.
- No `stg_typeform__forms` model despite forms data being in raw.

**Why it matters:** Lead-source attribution is blocked — we know a response came in, not *which form* it came from. Form-level segmentation (high-intent long form vs short tripwire form) is impossible today.

**Disposition:**
- **Phase A work:** (a) add `stg_typeform__forms` model — ~30 min of dbt work, pure additive, survives roadmap churn; (b) document in `gold-layer-roadmap.md` which business areas need form-level attribution.
- **Phase B work (U9 scope):** extractor fix to populate `form_id` on responses. Plus any cross-form analytics marts.
- **Out of scope:** none — all tractable.

**Priority:** P1 for `stg_typeform__forms`; P2 for the `form_id` extractor fix (depends on whether roadmap demands it).

---

## Gap 5 — Stripe 7 entity-level gaps (deliberately deferred)

**What exists:** 9 Stripe object_types in raw blob (7,619 rows total, 50-day stale). Only `stg_stripe__charges` and `stg_stripe__customers` modelled.

**What's missing:** No staging models for `balance_transactions`, `invoices`, `subscriptions`, `products`, `prices`, `disputes`, `refunds`.

**Why it matters:** Historical-only questions about Stripe-era D-DEE revenue are partially blocked by lack of staging. But the account is banned (per memory); nothing new will land.

**Disposition:**
- **Phase A work:** document-only — decide in Gold-layer roadmap whether any historical Stripe data is load-bearing for the business-area-map. If yes → schedule staging. If no → strike these from scope entirely.
- **Phase B work:** if needed, build staging for whichever Stripe entities the roadmap demands.
- **Out of scope:** Stripe backfill repair (moot; account banned).

**Priority:** P2 — ignore unless the Gold roadmap explicitly demands historical payment data.

---

## Gap 6 — Pipeline health: `bq-ingest` broken (pipeline-health)

**What exists:** GHL staging models read from Phase-2 raw tables that are frozen since 2026-04-19. Fathom raw is fresh (different writer). Warehouse-healthcheck scheduler failing with error code 13.

**What's missing:** A functioning `bq-ingest` Cloud Run Service. It returns HTTP 200 but doesn't land rows. Repair lives in the GTM repo, not Merge.

**Why it matters:** Every GHL-dependent business area reads 3-day-stale data. "Current state" dashboards are already wrong.

**Disposition:**
- **Phase A work:** Surface the blocker in `gold-layer-roadmap.md` preconditions. Document which marts cannot meet SLA until `bq-ingest` is fixed.
- **Phase B work (paused):** `bq-ingest` repair itself (U4b). Blocked by Strategic Reset pause on the GCP consolidation plan.
- **Out of scope:** Can't be fixed from this repo.

**Priority:** P0 blocker (by impact) but **P∞ for Phase A** (not fixable from here).

---

## Gap 7 — Slack ambiguity (source-level, but may not be a source)

**What exists:** Nothing under `raw_slack`. Single reference in `.claude/rules/metabase.md` mentions Slack as an alert *sink*, not a source.

**What's missing:** Clarity on whether Slack is supposed to be a data source.

**Disposition:**
- **Phase A work:** one-question clarification with David. Three intents possible (ingest / alerting / archive) — only the first is a new source. See `source-inventory.md` §7.

**Priority:** P1 on scoping — should be resolved by sprint day 2 so the coverage-matrix isn't drawn with a phantom column.

---

## Gap summary — priority-ranked

| # | Gap | Kind | Phase | Priority | Sprint-addressable? |
|---|---|---|---|---:|---|
| 1 | Fanbasis zero staging | Source-level | A | P0 | ✅ full |
| 2 | GHL Phase-2 vs legacy decision | Entity + hollow | A | P0 | ✅ decision-only |
| 6 | `bq-ingest` broken | Pipeline-health | Out-of-scope here | P0 (impact) | ❌ surface only |
| 3 | Fathom transcripts absent | Column-level | A partial | P1 | ✅ document-only |
| 4 | Typeform `forms` + `form_id` | Column + entity | A partial | P1 | ✅ partial (forms model) |
| 7 | Slack ambiguity | Source-level | A | P1 | ✅ clarify |
| 5 | Stripe 7 unmodelled entities | Entity-level | A decision-only | P2 | ✅ document-only |

---

## Cross-reference

- Source-by-source detail: `docs/discovery/source-inventory.md`
- Model-by-model status: `docs/discovery/staging-models.md`
- Phase gates + reversibility test: `docs/methodology.md` § Part 2
- Exec-level TL;DR across all four sprint artifacts: `docs/discovery/insights-summary.md`
