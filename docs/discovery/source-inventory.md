# Source Inventory — Data Discovery Sprint (2026-04-24)

**Sprint artifact 1.** Seeds from `docs/_archive/gtm-gcp-inventory.md` §7 / §8 / §10 / §11.

All row counts and freshness snapshots are from the 2026-04-23 U1 preflight against `project-41542e21-470f-4589-96d`. They are not re-queried for this document — if anything looks suspect, re-run the SQL in §7 of the preflight.

**Legend:** ✅ Live · 🟡 Partial · 🔴 Broken · ⚪ Not Started

---

## Tier 1 — Deep analysis

### 1. Calendly — 🟡 Partial (live data, narrow coverage)

**Status:** Live data landing, limited to 3 entity types, historical-only for pre-consolidation volume.

**Raw tables (consolidated project, `project-41542e21-470f-4589-96d`):**

| Table | Rows | Last write | Notes |
|---|---:|---|---|
| `Raw.calendly_objects_raw` | 10,882 | 2026-04-23 17:33 (fresh) | Single-blob landing, entity_types `scheduled_events`, `event_invitees`, `event_types` |
| `raw_calendly.*` (6 tables) | 0 | — | Phase-2 scaffold exists but no data has ever landed |
| `Raw.calendly_webhook_events_raw` | 1 | 2026-04-02 | One-off artifact, ignore |

**Staging models (3):** `stg_calendly__events`, `stg_calendly__event_invitees`, `stg_calendly__event_types`. All materialize against the single-blob landing via the U3 shim. `fct_calls_booked` + `fct_speed_to_lead_touch` depend on `stg_calendly__events`.

**Extractor method:** Cloud Run poller writing to `Raw.calendly_objects_raw`. Schedulers firing hourly and succeeding — this is one of only two sources that is genuinely fresh. The Phase-2 per-object extractor (`raw_calendly.calendly__*_raw`) was scaffolded but never activated.

**Known issues / limitations:**
- No cancellation / reschedule / no-show signal modelled in staging. The payload carries it; we don't extract it.
- No SDR-assignment data at Calendly grain. Assignment lives in GHL.
- Host / event_type metadata under-modelled — `stg_calendly__event_types` exists with no downstream consumer.
- Phase-2 scaffolding is dead weight — decide during sprint whether to demolish it or resurrect it.

**Recommendation: EXPAND.** Calendly is the system-of-record for the locked Speed-to-Lead metric and the most reliable fresh feed we have. For the business-area map, treat Calendly as the anchor for anything touching appointment volume, show-rate, no-show rescue, and SDR funnel. Specific things to probe during the sprint: (a) is there a cancellation entity_type we're not landing? (b) can we correlate Calendly `event_type` to pipeline/offer? (c) is the webhook_events table worth reviving for real-time event cancellation signals?

---

### 2. GoHighLevel (GHL) — 🟡 Partial (dual-path, stale, empty entities)

**Status:** Fullest surface area, most operationally critical, **currently the most broken**. Both the legacy and Phase-2 paths are stale (3–4 days behind); four Phase-2 entity types are zero-row.

**Raw tables — legacy single-blob:**

| Table | Rows | Last write | Notes |
|---|---:|---|---|
| `Raw.ghl_objects_raw` (all entity_types) | 68,314 | 2026-04-20 17:20 (3 days stale) | Wider coverage than Phase-2; `conversations` blob has 1,314 rows here |

**Raw tables — Phase-2 per-object (`raw_ghl.*`):**

| Table | Rows | Last write | Notes |
|---|---:|---|---|
| `raw_ghl.ghl__contacts_raw` | 15,888 | 2026-04-19 14:33 (4 days stale) | Core identity backbone |
| `raw_ghl.ghl__opportunities_raw` | 25,959 | 2026-04-19 14:33 | Pipeline + stage data |
| `raw_ghl.ghl__conversations_raw` | **101** | 2026-04-19 14:34 | **92% undercount vs legacy blob (1,314)** |
| `raw_ghl.ghl__outbound_call_logs_raw` | 100 | 2026-04-19 14:26 | Low row count; suspect also undercount |
| `raw_ghl.ghl__messages_raw` | 0 | — | Empty on both paths — extractor never carried this entity |
| `raw_ghl.ghl__notes_raw` | 0 | — | Empty on both paths |
| `raw_ghl.ghl__tasks_raw` | 0 | — | Empty on both paths |
| `raw_ghl.ghl__users_raw` | 0 | — | Empty on both paths |

**Staging models (6):** `stg_ghl__contacts`, `stg_ghl__conversations`, `stg_ghl__messages`, `stg_ghl__opportunities`, `stg_ghl__pipelines`, `stg_ghl__users`. The U3 column-rename sits at the source CTE (`entity_id AS id`, `to_json_string(payload_json) AS payload`); everything downstream is untouched. `messages`/`users` staging files exist but compile against zero-row raw.

**Extractor method:**
- **Phase-2 incremental:** Cloud Run Job `ghl-incremental-v2`, invoked hourly by Cloud Scheduler. **Frozen since 2026-04-19 14:31** — no executions despite scheduler firing.
- **Legacy blob writer:** Cloud Run Service `bq-ingest` (endpoint `/ingest-ghl`), invoked hourly by `ghl-hourly-ingest` scheduler. Service returns HTTP 200, but no rows land. Service-level regression.
- Same `bq-ingest` service is implicated in Fathom + warehouse-healthcheck scheduler error code 13 (INTERNAL).

**Known issues / limitations:**
- **`bq-ingest` service broken** — highest-priority blocker for anything GHL-dependent. Fix lives in the GTM repo, not Merge.
- **92% undercount on conversations** (101 vs 1,314) — makes any Speed-to-Lead numerator computed from Phase-2 unreliable. Legacy blob is authoritative today.
- **Four empty entity types** (messages, notes, tasks, users) — no shim can recover them; upstream extractor never wrote them.
- **SDR identity lives in a seed**, not GHL (`2-dbt/seeds/ghl_sdr_roster.csv`) because GHL's `account/agency` and `user/admin` fields don't distinguish SDR from AE.
- **Roster gaps:** Ayaan + Jake have `unknown` roles; Moayad (departed) + Halle (active Closer) missing from seed entirely (per `docs/proposals/roster_update_from_oracle.md`).
- **PIT token was exposed** in a Claude transcript 2026-04-19; rotation in GHL owed.

**Recommendation: INVESTIGATE (highest priority).** GHL is the richest business-intelligence surface in the stack (contacts, opportunities, conversations, pipeline stages, tags, SDR assignment) and the most broken. During the sprint specifically probe: (a) what does the `raw_ghl.ghl__conversations_raw` 101-row slice actually contain — is it a filter or a bug? (b) which of the zero-row entities (notes, tasks, users especially) do business areas actually need? (c) can legacy blob be treated as the source of truth for the Gold rebuild and Phase-2 demolished? (d) what's the `bq-ingest` repair path? This is the single biggest unlock.

---

### 3. Fanbasis — 🟡 Partial (live raw, unmodelled, stale config)

**Status:** Discovery corrected earlier project-state language that called Fanbasis "broken on both sides." Raw data IS landing; the Merge dbt layer has never been wired to it.

**Raw tables:**

| Table | Rows | Last write | Notes |
|---|---:|---|---|
| `Raw.fanbasis_transactions_txn_raw` | 455 | 2026-04-23 07:27 (fresh) | Daily landing working |

**Staging models:** **zero.** `2-dbt/models/staging/fanbasis/_fanbasis__sources.yml` is a placeholder with `tables: []` and still points at `database: dee-data-ops` — the deprecated pre-U2 project. No `stg_fanbasis__*.sql` files exist.

**Extractor method:** Custom extractor landing daily at 07:27 UTC — inferred from freshness pattern. Actual Cloud Run Job name and schedule not captured in U1 preflight for Fanbasis specifically; verify during sprint. Note: `1-raw-landing/fanbasis/` was described as stubbed in CLAUDE.local.md Week-0 asks, but data is clearly landing, so either the stub got finished quietly or another writer is doing it.

**Known issues / limitations:**
- **No Merge-side staging model.** The raw landing is invisible to dbt today.
- **`_fanbasis__sources.yml` still declares the pre-U2 project** — needs to be retargeted to `project-41542e21-470f-4589-96d` before any staging model can compile.
- **API docs / credentials owed by D-DEE** (per Week-0 asks) — affects extractor maintenance, not current landing.
- **Coverage scope unknown.** Only `transactions_txn` lands; refunds, chargebacks, products, customers are not confirmed present. 455 rows in 23 days suggests ~20 txn/day, which is plausible for a live payment processor.

**Recommendation: INVESTIGATE → EXPAND.** Per memory `project_stripe_historical_only.md`, Fanbasis is the **live revenue source** for D-DEE while Stripe is historical-only due to account ban. This elevates Fanbasis to the most strategically important financial data path — but we haven't modelled any of it. In the sprint: (a) audit `Raw.fanbasis_transactions_txn_raw` payload shape — what fields are there? (b) confirm which additional Fanbasis entities (refunds, chargebacks, customers) should land next; (c) retarget `_fanbasis__sources.yml` to the consolidated project; (d) build a single exploratory `stg_fanbasis__transactions` view to make the raw queryable. The Gold-layer roadmap's revenue / churn / refunds marts all depend on this.

---

## Tier 2 — Moderate analysis

### 4. Typeform — 🟡 Partial (fresh responses, missing `form_id`, broken daily backfill)

**Status:** Live for responses; backfill job failing daily; key identity column missing.

**Raw tables:**

| Table | Rows | Last write | Notes |
|---|---:|---|---|
| `Raw.typeform_objects_raw` (responses + forms) | 5,085 | 2026-04-23 15:59 (fresh for responses) | Single-blob landing |

**Staging models (1):** `stg_typeform__responses`. No `stg_typeform__forms` model despite `forms` entity_type present in raw.

**Extractor method:** Real-time hourly writer (working) + daily `typeform-backfill` Cloud Run Job scheduled 05:15 UTC. **Backfill failing exit code 2 for 3+ consecutive days** (2026-04-21, -22, -23). Root cause not captured in preflight; container error.

**Known issues / limitations:**
- **`form_id` is NULL in staging.** The GTM extractor does not write a `form_id` column and does not embed it in `payload_json` for responses. `not_null` test on `form_id` was lifted in U3 with restore-at-U9 note. Downstream `dim_contacts` sees nulls.
- **No `stg_typeform__forms` model** despite the entity_type being landed — only responses are queryable from dbt.
- **Backfill broken for 3+ days.** Real-time hourly writes keep coming; backfill failure means historical repair runs aren't happening.

**Recommendation: KEEP AS-IS for sprint; schedule extractor rewrite.** Responses are fresh enough for discovery work. The `form_id` gap is known and the restore lives in U9 (post-sprint). For the sprint, confirm: (a) which business areas depend on knowing *which form* a response came from (lead-source attribution is the obvious one); (b) are there other non-response entities (partial submissions, hidden fields, webhook events) we should be landing? (c) does the backfill failure matter for the volume we care about?

---

### 5. Fathom — 🟡 Partial (metadata only, zero transcripts, analysis dead)

**Status:** Call metadata landing fresh; transcript payload entirely missing; LLM analysis pipeline dead for ~3 weeks.

**Raw tables:**

| Table | Rows | Last write | Notes |
|---|---:|---|---|
| `Raw.fathom_calls_raw` | 1,157 | 2026-04-23 18:17 (fresh) | 0% transcript coverage |
| `Raw.fathom_call_intelligence` | **0** | — | LLM analysis output — table exists but empty |

**Staging models (1):** `stg_fathom__calls`. First Fathom model in Merge (added U3). Uses extractor-populated top-level columns plus JSON-decoded `payload_json` fields.

**Extractor method:**
- `fathom-hourly-ingest` Cloud Run Service (via `bq-ingest`) — lands call metadata, **working** (hourly fresh). Transcripts absent from payload.
- `fathom-backfill` Cloud Run Job daily at 04:00 UTC — failing exit code 1 for 3+ days.
- `fathom-llm-analysis` Cloud Run Job — **dead since 2026-04-03.** Responsible for writing `Raw.fathom_call_intelligence`. Two weeks of silence.

**Known issues / limitations:**
- **0% transcript coverage across all 1,157 calls** — `payload_json.$.transcript` is absent. Root cause hypotheses (per preflight §9): (a) extractor only calls `/meetings` not `/transcripts`, (b) Fathom API requires an async transcript job, (c) auth scope missing. Diagnosis deferred to U6.
- **LLM analysis pipeline dead 3+ weeks.** Downstream `fathom_call_intelligence` table empty. Any Gold mart depending on call sentiment / classification / revenue-relevance is blocked.
- **Top-level columns (`is_revenue_relevant`, `classification_*`)** are populated by the hourly extractor and queryable via `stg_fathom__calls`, so basic call-volume and classification analysis works today.

**Recommendation: INVESTIGATE for sprint.** Fathom is likely to show up in the business-area map for at least three areas: AE call quality, closing performance, revenue attribution (via `is_revenue_relevant`). The transcript gap and LLM-analysis death are two different problems — separate them. In the sprint: (a) treat `stg_fathom__calls` as usable today for metadata-only questions; (b) scope the transcript root-cause investigation as a sprint output (not a fix), so U6 starts with the diagnosis rather than a blank page; (c) decide whether the LLM analysis pipeline is worth reviving or whether a Gold-layer mart should compute classification from transcripts (once they land).

---

## Tier 3 — Light summary

### 6. Stripe — 🔴 Broken (historical-only by design; all paths stale ~50 days)

**Status:** D-DEE's Stripe account is **banned** (per memory `project_stripe_historical_only.md`). Stripe is useful only as a historical reference — Fanbasis is the live revenue source. Daily backfill job failing daily on top of that.

**Raw tables:**

| Table | Rows | Last write | Notes |
|---|---:|---|---|
| `Raw.stripe_objects_raw` (9 object_types) | 7,619 | 2026-03-04 00:38 (~50 days stale) | All object_types uniformly stale |

Breakdown (from preflight §10): charges 3,375 · balance_transactions 1,934 · invoices 966 · customers 516 · subscriptions 420 · products 185 · prices 181 · disputes 21 · refunds 21.

**Staging models (2):** `stg_stripe__charges`, `stg_stripe__customers`. Both via U3 blob-shim.

**Extractor method:** `stripe-backfill` Cloud Run Job — failing container error intermittently; last 2 runs failed. No other Stripe writer active.

**Known issues / limitations:** Account banned; no new transactions. 50-day staleness is a GTM-repo bug but also largely moot — there's nothing new to land. Historical data is complete up to ~2026-03-04.

**Recommendation: KEEP AS-IS.** Don't spend sprint time here. In the business-area map, Stripe answers **only historical questions** (what happened pre-ban), and even those max out at 2026-03-04. Any "current revenue" / "current churn" / "current refunds" question routes to Fanbasis, not Stripe. Cross-reference in the crosswalk matrix as "historical-only." Decide in the Grok roadmap whether Stripe deserves its own mart or whether its data folds into a unified `revenue_history` mart alongside Fanbasis.

---

### 7. Slack — ⚪ Not Started (not a data source in this stack)

**Status:** **Not a data source.** Clarifying question for the user.

**What exists:** The only `slack` reference in the repo is `.claude/rules/metabase.md`, which mentions Slack as one of Metabase's alert notification channels (alongside email). That is a *dashboard output destination*, not a *data source*.

**What does NOT exist:**
- No `raw_slack` dataset.
- No `Raw.slack_*_raw` tables.
- No `2-dbt/models/staging/slack/` directory.
- No Slack extractor, Cloud Run Job, or scheduler entry in the U1 preflight inventory.
- No Slack access / credential mention in CLAUDE.local.md or the engagement scope.

**Recommendation: CLARIFY with the user before sprint day 2.** Three plausible intents behind "Slack" as source #7:

1. **Slack as ingest source** — e.g., scraping a `#sdr-activity` or `#sales-floor` channel for SDR commentary, AE hand-offs, deal-desk discussion. Would require a Slack app install, OAuth, and an extractor. Not in scope anywhere today.
2. **Slack as alerting sink** — already informally on the Metabase side per the rule file. Not a data source; a notification channel. No ingestion needed.
3. **Slack as ops-channel archive** — e.g., client-facing comms with D-DEE. Privacy-sensitive; would need explicit client approval.

If the user confirms intent (1), it becomes a ⚪ Not Started source requiring its own extractor build and would be a candidate for the Grok roadmap's prioritization. If intent (2) or (3), strike it from the source list and don't carry it into the coverage matrix. Do not invent data where none exists.

---

## Summary

### Source health at a glance

| # | Source | Tier | Status | Live data | Staging models |
|---|---|---|---|---|---:|
| 1 | Calendly | 1 | 🟡 Partial | ✅ Fresh | 3 |
| 2 | GHL | 1 | 🟡 Partial | 🔴 Stale (bq-ingest broken) | 6 |
| 3 | Fanbasis | 1 | 🟡 Partial | ✅ Fresh | 0 |
| 4 | Typeform | 2 | 🟡 Partial | ✅ Fresh (responses) | 1 |
| 5 | Fathom | 2 | 🟡 Partial | ✅ Fresh (metadata only) | 1 |
| 6 | Stripe | 3 | 🔴 Broken | 🔴 50-day stale (account banned) | 2 |
| 7 | Slack | 3 | ⚪ Not Started | n/a (not a data source) | 0 |

**Fully healthy sources (both landing and staging green, no known issues):** **0 of 7.** Every source has at least one active caveat. The closest-to-healthy source is Calendly (fresh, wired, but narrow coverage). The next-closest is Typeform (fresh responses, backfill broken, `form_id` gap).

### Critical blockers (ranked by impact)

1. **`bq-ingest` Cloud Run Service regression** — the single biggest blocker. Affects GHL legacy + Phase-2 writes + Fathom hourly + warehouse-healthcheck. GHL is the richest business-signal source in the stack; its 3–4 day staleness means any "current state" question routed through GHL is already wrong. Fix lives in the GTM repo, not Merge.
2. **Fanbasis has no Merge-side staging at all** — the only live revenue source in the engagement is invisible to dbt. One `stg_fanbasis__transactions` view would unlock every revenue-area business question.
3. **Fathom 0% transcript coverage** — blocks everything depending on call content, classification, or sentiment.
4. **GHL 4 empty Phase-2 entities** (messages, notes, tasks, users) — no shim can recover them; upstream extractor never wrote them. Decide during sprint which, if any, matter for business areas.
5. **GHL conversations 92% undercount** (101 Phase-2 vs 1,314 legacy) — makes Phase-2-only analyses unreliable; forces reliance on the legacy blob.
6. **Stripe ~50-day staleness** — mostly moot given the account ban, but cross-check any historical-revenue question against the 2026-03-04 cutoff.

### Quick wins available (low effort, high leverage for sprint)

1. **Retarget `_fanbasis__sources.yml` to the consolidated project and add `stg_fanbasis__transactions`.** Unblocks revenue-area discovery without any extractor work. ~1–2 hours of dbt work, zero new infrastructure. This is the single highest-leverage move available today.
2. **Audit `Raw.fanbasis_transactions_txn_raw` payload shape.** A 10-minute BQ query to list the JSON keys in a sample payload tells us what fields we have and what fields we'd need an extractor change for.
3. **Decide the GHL Phase-2 vs legacy question.** If legacy is authoritative and Phase-2 is 92%-undercounting conversations + 4-entities-empty, the cleanest path is to point all GHL staging at the legacy blob via shim (like Stripe/Typeform/Calendly/Fathom already do) and retire Phase-2 entirely. This resolves the undercount, the empty-entity gap, and the dual-source confusion in one move.
4. **Clarify Slack intent with the user.** One-question decision — either Slack becomes a real source to plan for, or it comes off the list.
5. **Add `stg_typeform__forms`.** Forms data is in raw; surfacing it in dbt is a ~30-min job. Likely needed for any form-level segmentation.
6. **Demolish dead Phase-2 Calendly scaffolding** if the decision in (3) is to consolidate on the legacy blob — it's confusing for onboarding new readers of the dbt project.

### What the sprint should NOT try to fix here

- `bq-ingest` repair (GTM repo, not Merge).
- Stripe backfill (moot; account banned).
- Fathom LLM analysis revival (separate architectural decision, scoped to U6 post-sprint).
- Fathom transcript root-cause fix (scoped to U6; sprint produces the diagnosis, not the fix).
- Typeform `form_id` extractor change (scoped to U9 post-sprint).

### Next artifact

`docs/discovery/business-area-map.md` — enumerates business areas D-DEE operates and maps stakeholder / decision / data deps / current state. Seeds from this inventory.
