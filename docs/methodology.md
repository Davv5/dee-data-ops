# Methodology

Canonical source of truth for how we build data-ops projects. LLM-first: rules are declarative and self-contained. For human-friendly narrative, see `README.md`.

Last updated: 2026-04-24.

---

## How to use this file

**Audience:** Claude agents (primary) and human operators (secondary) working on any data-ops engagement in this repo or a copy of it.

**Reading order:**
1. Part 1 — the data contract (architecture rules, always active).
2. Part 2 — operating principles (phase-gated; check which phase you're in before applying).
3. Part 3 — per-engagement artifact templates (fill in per project).

**Drift resolution:** If a scoped rule in `.claude/rules/*.md` contradicts this file, this file wins. Update this file first; then update the scoped rule to match.

---

# Part 1 — The 3-Layer Architecture

Data moves strictly left-to-right: **raw landing → staging → warehouse → marts → BI**. Each layer has one job. Skipping a layer is forbidden.

## Cross-layer invariants

**Rule:** Data flows in one direction only: raw → staging → warehouse → marts.
**Why:** Backflow creates cycles; downstream fixes that mutate upstream break idempotency.
**Example:** A mart column derived from a customer segmentation MUST NOT write back to `stg_ghl__contacts`.

**Rule:** Only staging models select from `source()`. No other layer reads raw directly.
**Why:** Source-schema drift is contained to one layer.
**Example:** `dim_contacts.sql` selects from `stg_ghl__contacts`, never from `raw_ghl.contacts`.

**Rule:** Each layer MUST pass all tests before downstream layers build.
**Why:** Errors caught at the layer boundary stay local; errors that pass through corrupt everything downstream.
**Example:** A duplicate surrogate key in warehouse MUST stop the build before marts attempt to consume it.

**Rule:** No layer may skip the next. Raw cannot feed warehouse. Staging cannot feed marts.
**Why:** Skipping forfeits the guarantees the skipped layer provides (cleanup, business logic, presentation shape).

---

## Layer 1 — Raw Landing

**Purpose:** Capture source data unchanged, schema-drift-proof.

**Rules:**
- Raw tables MUST be named `raw_<source>.<table>` (BigQuery dataset-qualified).
- Raw tables for custom extractors MUST use the schema `(id STRING, _ingested_at TIMESTAMP, payload STRING)` where `payload` is a JSON string.
- Fivetran-managed sources MAY land with native typed columns.
- Raw tables MUST NOT be mutated by dbt. Only extractors write to raw.
- Raw tables MUST NOT be read by any model except staging.

**Why (JSON-string payload pattern):** Upstream APIs (e.g., GHL) have mixed-type nested fields that break BigQuery schema autodetect on the second row. JSON-string landing is drift-proof; staging parses via `JSON_VALUE` / `PARSE_JSON`.

**Example:** `raw_ghl.contacts` — `id STRING, _ingested_at TIMESTAMP, payload STRING`.

**Failure mode prevented:** Schema drift in a source API does not stop ingestion or require redeploying extractors.

---

## Layer 2 — Staging

**Purpose:** 1:1 cleanup of raw tables. No business logic.

**Rules:**
- Staging models MUST be materialized as `view`.
- Staging models MUST be named `stg_<source>__<table>.sql` (double underscore between source and table).
- Staging models MUST NOT change row count versus source.
- Staging models MUST NOT join tables.
- Staging models MUST alias columns to `snake_case` and cast to canonical types.
- Staging models MUST be the only layer that selects from `source()`.
- Only one staging model per raw table.

**Why:** Staging is the single place where raw-column names, types, and quirks are normalized. Every downstream model reads staging, so one fix propagates.

**Example:** `2-dbt/models/staging/ghl/stg_ghl__contacts.sql` — reads `source('ghl','contacts')`, parses JSON, aliases columns.

**Failure mode prevented:** Renaming logic duplicated across marts, causing silent drift when one mart is updated and others aren't.

---

## Layer 3 — Warehouse

**Purpose:** Apply business logic. Organize into a dimensional star schema.

**Rules:**
- Dimension tables MUST be named `dim_<entity>.sql` (e.g., `dim_contacts`, `dim_pipeline_stages`).
- Fact tables MUST be named `fct_<event>.sql` (e.g., `fct_calls_booked`).
- Fact tables MUST hold surrogate keys that join to dimension tables.
- Fact tables MUST NOT join directly to other fact tables.
- Dimension tables hold descriptive context ("nouns and adjectives"): who, what, where.
- Fact tables hold measurable events ("verbs"): what happened, when, how much.
- Warehouse models MUST declare data-quality tests in YAML (`unique`, `not_null`, relationship tests at minimum) on all surrogate keys.
- Warehouse models MAY be `table` or `incremental` materialization; prefer `table` unless volume demands incremental.

**Why (no fact-to-fact joins):** Direct fact-to-fact joins create accidental many-to-many relationships and row duplication. Always route through a shared dimension.

**Example:** `fct_calls_booked.calendly_event_id` is the grain; it joins `dim_contacts`, `dim_pipeline_stages`, `dim_sdr_roster` via surrogate keys.

**Failure mode prevented:** A duplicate transaction or missing product ID from the source is caught by `dbt test` before it infiltrates downstream marts.

---

## Layer 4 — Marts

**Purpose:** Presentation zone for BI tools. Decoupled from warehouse technical structure.

**Rules:**
- Mart models MUST use business-friendly names (not `fct_` / `dim_` prefixed). Example: `speed_to_lead_detail`, not `fct_sla_grain`.
- Marts MUST be wide, denormalized tables produced by joining facts to dimensions.
- Marts MUST NOT require downstream consumers to understand the warehouse topology.
- Marts MAY expose any grain needed by BI (daily rollup, per-event detail, etc.). Document the grain in model YAML.
- BI tools (Metabase, Evidence.dev) MUST connect to marts only. They MUST NOT read warehouse or staging directly.

**Why:** The mart layer is the contract with business users. Renaming `fct_calls_booked` to "Sales Overview" at this layer isolates the business vocabulary from the engineering vocabulary.

**Example:** `speed_to_lead_detail` — wide table joining `fct_calls_booked`, `dim_contacts`, `dim_sdr_roster`, `dim_pipeline_stages` into one row per booked call with all descriptive context.

**Failure mode prevented:** Business users building dashboards that break on every warehouse refactor because they coupled to internal model names.

---

## Operational trigger

**Rule:** `dbt build --target <env>` is the single command that executes the full pipeline.
**Why:** One trigger compiles SQL, materializes staging views, builds warehouse, runs YAML tests, deploys marts. No step is skippable by convention.
**Example:** `dbt build --target dev -s tag:staging+` builds staging and everything downstream.

**Rule:** CI runs `dbt build --target ci` on every PR. Prod deploys run `dbt build --target prod` post-merge.
**Rule:** Direct `--target prod` invocation from a local shell is blocked by hook (`.claude/settings.json`). Prod runs through CI only.

---

# Part 2 — Operating Principles

## The thesis: maximum safe speed

**Rule:** Build the decision-making system that lets you move at maximum safe speed.
**Why:** Velocity is the goal, but unexamined velocity on an uncalibrated compass amplifies error. Speed is a second-order output of crisp decisions, not a terminal objective.
**Derivation:** Inspired by Cat Wu (Head of Product, Claude Code) — "clear goals resolve ambiguity; team principles let anyone decide without blocking." Speed follows from decision clarity; it is not pursued directly.

## The reversibility test

**Rule:** Before taking an action, apply the reversibility test: *"If the Gold-layer roadmap changes in two weeks, does this cost rework or does it survive?"*
- If it survives → safe speed; ship it.
- If it costs rework → either defer, reduce blast radius, or ship it labeled as research preview.

**Why:** A concrete test turns "safe speed" from slogan into operational filter.

**Example mappings:**
- Adding a staging model for a newly discovered source → survives roadmap churn → safe speed.
- Building a new mart named in the current roadmap → safe speed.
- Building a mart not yet in the roadmap → defer OR ship as research preview.
- Client-facing dashboard labeled "research preview" → commitment-cost lowered → safe speed even on uncertain requirements.

## Research preview labeling

**Rule:** Ship uncertain client-facing capability as "research preview" until the client signs off on its behavior.
**Why:** Lowers commitment cost. Lets you get feedback in days instead of weeks. If the client rejects the shape, you have not promised it.
**Example:** v2 Evidence.dev cutover ships labeled "preview" until D-DEE confirms parity with v1.6.

## Phase A — Steering (current)

**Active when:** Gold-layer roadmap is not yet locked.

**Rules:**
- New marts MUST NOT be built. Staging, warehouse, and research-preview artifacts only.
- Every structural decision (new dimension, new grain, new business area coverage) MUST have a written justification in `docs/discovery/` or a session WORKLOG entry.
- Client-facing surface area MUST NOT grow. Fixes to existing surfaces OK.
- Discovery Sprint artifacts (`docs/discovery/`) are the only ceremonial artifacts allowed. Everything else is docs-only or internal.
- Decisions that touch multiple business areas MUST wait for the coverage matrix or be made explicitly as interim-until-matrix.

**Goal:** Produce the discovery packet with enough quality that Phase B decisions downstream are de-risked. The decision gates are source inventory, business-area map, coverage matrix, and Gold-layer roadmap; staging inventory, gap analysis, and insights summary are supporting views.

**Target exit date:** ~2026-05-08 (per active Strategic Reset plan).

## Phase B — Velocity (activates when Gold-layer roadmap is approved)

**Active when:** Gold-layer roadmap is signed off and the per-engagement Gold artifacts (§3) are filled in.

**Rules:**
- Any change reversible-in-two-weeks ships without written justification.
- Default path for shippable work: build directly on a branch, merge, move on. No multi-agent pipeline ceremony.
- Remove every barrier to shipping that doesn't add real value.
- Labeled research-preview capability ships freely; locked-contract capability gates on tests + reversibility test.
- PRDs are optional; team principles + metric readouts replace most of their function.

**Goal:** Maximize throughput on roadmap-aligned work.

**Source principles:** `.claude/rules/live-by-default.md`, `feedback_ship_over_ceremony.md`. These are Phase B principles and MUST NOT be applied during Phase A.

## The phase gate

**Rule:** At the start of every session, determine which phase is active. Read `.claude/state/project-state.md` — the "Active workstream" line names the phase.

**Rule:** If a request would violate the active phase's rules (e.g., a request to "ship a new mart" during Phase A), surface the phase-gate contradiction explicitly before acting. Do not silently rationalize.

**Rule:** Phase transitions require an explicit decision captured in WORKLOG.md (Phase A → B = "Gold-layer roadmap approved"; B → A = "new Strategic Reset declared"). Transitions are not implicit.

## Metric readouts

**Rule:** A metric readout (weekly digest to the client) MUST summarize: (a) the business outcome of the metric, (b) the direction it moved this week, (c) the two or three drivers, (d) the prioritization implication.
**Why:** The client reads the prioritization as much as the numbers. This is the mechanism for keeping the Gold-layer roadmap aligned.
**Example:** D-DEE weekly digest — "Speed-to-Lead landed at X% this week (+2pp). Drivers: SDR coverage expanded on Pipeline Y. Prioritization implication: defer Pipeline Z rollout until SDR coverage matches."

## Distributed decision-making (solo-operator translation)

**Rule:** Empower every future Claude session to decide without re-hydrating context. This file, `.claude/rules/`, `CLAUDE.md`, `CLAUDE.local.md`, and project-state.md together must answer "who · why · what we're willing to trade off" for the active engagement.
**Why:** In a team, distributed decisions prevent bottlenecks on one PM. In a solo engagement, the same mechanism prevents bottlenecks on context-rehydration across sessions.
**Failure mode prevented:** A future session asks "who is the user of this mart?" and cannot answer without interrupting David.

---

# Part 3 — Per-Engagement Artifact Templates

The Discovery Sprint artifacts every engagement produces. Filling the decision
artifacts is how Phase A completes; supporting artifacts make the decision
artifacts easier to audit.

## Source inventory (`docs/discovery/source-inventory.md`)

One row per source system. Columns:
- Source name
- Ingestion path (Fivetran / custom extractor / CSV / direct query)
- Raw landing status (live / stale / missing)
- Known data quality issues
- Owner (client-side person who controls access / schema)
- Criticality (blocker / important / nice-to-have per the business-area map)

## Staging-model inventory (`docs/discovery/staging-models.md`)

One row per staging model. Columns:
- Model name
- Source system
- Raw table or entity read
- Health status (fresh / stale / empty / placeholder)
- Known caveats
- Downstream dependencies, if any

## Gap analysis (`docs/discovery/gap-analysis.md`)

One row per gap between raw reality and modeled reality. Columns:
- Gap name
- Gap type (source-level / entity-level / hollow-model / column-level / pipeline-health)
- What exists today
- What is missing
- Business impact
- Disposition (Phase A / Phase B / out of scope)
- Priority

## Business-area map (`docs/discovery/business-area-map.md`)

One row per business area (e.g., lead funnel, payments, SDR performance, client retention). Columns:
- Area name
- Owning stakeholder(s)
- Key questions the area needs answered
- Required sources (cross-reference to source inventory)
- Known blockers
- Priority rank

## Coverage matrix (`docs/discovery/coverage-matrix.md`)

Grid: business areas × sources. Each cell states: "current coverage / target coverage / gap."
- "Current coverage" = what the pipeline produces today.
- "Target coverage" = what the area's key questions demand.
- "Gap" = the delta, sized small/medium/large.

## Gold-layer roadmap (`docs/discovery/gold-layer-roadmap.md`)

Prioritized list of marts to build in Phase B. Each entry:
- Mart name
- Business area served
- Grain
- Key columns / measures
- Dependencies (dims, facts, sources)
- Priority (P0 / P1 / P2)
- Research-preview vs. locked-contract classification
- Phase B unlock criteria (what must be true before this mart starts)

## Insights summary (`docs/discovery/insights-summary.md`)

Short executive summary across the discovery packet:
- Highest-leverage findings
- Data reality snapshot
- Strategic takeaways
- What remains pending
- Phase-transition criteria

**Rule:** Phase B does not activate until the Gold-layer roadmap is approved by the client (or by David in sole-operator engagements).

---

# Meta-rule

**Rule:** This file is authoritative. If any scoped rule file, CLAUDE.md, or memory contradicts this file, this file wins. The contradicting artifact MUST be updated in the same session the contradiction is found.
**Why:** Canonical source prevents drift; drift is what fragments the system's decision clarity over time.
**How to update:** Edit `docs/methodology.md` first. Then propagate to the scoped rule(s) in `.claude/rules/`. Then log the change in WORKLOG.md with the rationale.
