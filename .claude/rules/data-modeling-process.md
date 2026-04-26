---
paths: ["2-dbt/models/**"]
---

# Data Modeling Process

Load this rule when working on any file under `2-dbt/models/`. Complements the layer-shaped rules (`staging.md`, `warehouse.md`, `mart-naming.md`) with a process arc — what to think about, in what order, before opening a SQL editor.

Grounded in [Joshua Kim, "[AE] The Order in which I Model Data — The Thought Process in Analytics Engineer"](https://joshua-data.medium.com/my-analytics-engineering-process-en-435445038897), Medium, April 2026, plus this engagement's own lessons. Maxims here cite Kim by section; lessons-learned cite the WORKLOG entry that surfaced them.

> **The four maxims that explain everything else:**
> 1. **Grain + PK + relationships understanding is 80% of the work.** Skip this and you'll rewrite from scratch.
> 2. **`QUALIFY ROW_NUMBER() OVER (...) = 1` deduplication in lower layers is a symptom**, not a fix — the upstream PK / grain is wrong.
> 3. **WHERE conditions disappear top-to-bottom in a CTE chain.** A `WHERE` that surfaces in a lower CTE means the model has a bug — you filtered late instead of early, and that filter probably overfits.
> 4. **After every join, `COUNT(1)` and verify the row count.** Saves hours of debugging.
> (source: "[AE] The Order in which I Model Data" — Joshua Kim, Medium)

---

## The 6 steps

### Step 1 — Source table navigation

Before writing any SQL, understand the source data thoroughly. Read what each Raw table actually contains, what the columns mean in business terms, what the rows represent.

Things to check:

- Row grain — what is one row? Is it one event, one entity, one event-per-entity-per-day?
- Column nullability + value distributions — which columns are dense, which are sparse, which have weird sentinel values
- Implicit relationships — which columns join to which other tables; which join keys are unique vs which fan out
- Source freshness + landing pattern — are these incremental, snapshot, or replace-each-load
- Domain context — what does the user actually do in the app/web that produces this row? **If you don't understand the domain, AI cannot understand it either.** (Kim, Step 1)

For this engagement specifically, the seed for source navigation is `docs/_archive/gtm-gcp-inventory.md` (Port plan U1). Extend, do not recreate.

> ⚠️ Skipping Step 1 is the most common reason a model gets rewritten from scratch — you find out after you've built the warehouse layer that the grain assumption was wrong. (source: Kim Step 1; corroborated by 2026-04-22 Speed-to-Lead F1/F2 refactor — see `mart-naming.md` lessons-learned.)

### Step 2 — Identify business needs and determine final form

Listen to the data user. What metric do they need? What slices? What time grain? What are they actually going to do with the answer?

Then decide the **final-layer shape**:

- Mart name (business-friendly per `mart-naming.md`)
- Mart schema (which columns belong)
- **Confirm the fact-table grain.** Date × cohort? user_id? order_id × item_id? **This is the most load-bearing decision in the whole process.** If you're not sure, you'll keep coming back to this step from later steps. (Kim, Step 2)

> **Grain is the single most important decision.** In this repo it is captured upfront in:
> - The plan document (e.g. `docs/plans/<name>-plan.md` Requirements Trace)
> - `_<dir>__docs.md` for the model
> - The mart entry in `docs/discovery/gold-layer-roadmap.md` (Strategic Reset Sprint output): grain + PK + 1-line purpose for every candidate mart.

### Step 3 — Source → Final-layer query logic design

Whiteboard the data flow. Don't write SQL yet — sketch:

- Which source tables feed which staging models
- Which staging models feed which warehouse facts/dims
- Which warehouse models feed the mart
- Where joins happen and what the join keys are
- Where aggregation happens

The goal: you should be able to draw the lineage on paper before touching a SQL editor.

In this repo, the existing Mermaid lineage diagrams in `2-dbt/dbt_project.yml`-driven `dbt docs` are the reference; for new marts, sketch in the plan document first.

### Step 4 — Silver Layer (warehouse) physicalization decision

For each intermediate model, decide whether to physicalize (materialize as table) or leave as a view.

**Physicalize when:**

- Frequent reuse across many downstream models (one expensive computation, many readers)
- Expensive computation (large window functions, big joins, heavy aggregations)
- The downstream needs a stable point-in-time snapshot

**Don't physicalize when:**

- Used by exactly one downstream model
- Cheap to recompute
- The data is highly volatile and a stale physicalized copy is worse than a fresh recompute

In this repo, the directory-level defaults already do most of this work (`staging/` = view, `warehouse/` = table, `marts/` = table — see `dbt_project.yml`). The judgment call is on **intermediate / `models/intermediate/`-shaped models** that don't fall under the directory defaults.

> ⚠️ **Silver Layer scalability is the hardest thing to fix later.** If you build many narrow, similar tables instead of one wide reusable one, your downstream models proliferate and you accumulate refactor debt that nobody can pay off. Build for scalability from the first model, even if it feels overkill. (Kim, Step 4)

### Step 5 — Write the SQL

Once Steps 1-4 are settled, the SQL is largely mechanical. The conventions in `docs/conventions/dbt_style_guide.md` cover the SQL-style maxims (CAST vs SAFE_CAST, preprocessing in first CTE, unified naming at top CTE, variableized constants, FULL OUTER UNION ALL BY NAME, fact × dim uniqueness check, WHERE filtering top-down, verify row count after joins). This rule's contribution at Step 5 is the **process discipline**: name the partition, name the materialization, name the incremental strategy, BEFORE writing the SELECT.

> ⚠️ **`QUALIFY ROW_NUMBER() OVER (...) = 1` in a downstream model is a code smell.** It usually means the upstream model's PK or grain is wrong. Fix the upstream model rather than papering over it downstream. Once is fine; twice is a pattern; three times is a debt. (source: Kim "last tip"; codified in this rule because it surfaces in nearly every multi-day debugging session.)

### Step 6 — Check after writing

Before merging:

- **Clustering review.** For tables with frequent filtering on a column, consider BigQuery clustering — but only when the table is large enough that scan cost outweighs the load-side cost of clustering. Don't cluster reflexively.
- **Naming review.** Column names + table name human-readable? Will this confuse a stakeholder who doesn't speak Kimball?
- **Catalog write-up.** Update `_<dir>__docs.md`. Concise (AI agents and humans both read it; long descriptions hurt search precision in NL2SQL skills). State the grain and the PK in the description.
- **Six-perspective DQ check** (per `warehouse.md` testing minima):
  1. Uniqueness — `unique` test on PKs and surrogate keys
  2. Nullability — `not_null` on join keys and load-bearing columns
  3. Referential integrity — `relationships` test on every fact FK
  4. Range — `dbt_expectations` value-range test for known bounded columns
  5. Freshness — `freshness` block in source YAML; `dbt_expectations.expect_row_values_to_have_recent_data` for derived models
  6. Volume — row-count-vs-prior-run test for incremental tables

> **Test as far upstream as possible (Bronze/Silver), not in marts.** Mart-level tests during every batch are expensive maintenance; staging/warehouse tests catch issues at the source. Mart deployment runs unit-test-grade verification once before deploy, not on every refresh. (source: Kim Step 6.)

---

## The four macro maxims (re-stated for emphasis)

Because they explain so much downstream pain, the four maxims from the top of this rule are repeated here with their failure modes:

### 1. Grain + PK + relationships = 80% of the work

If you spend less than 30% of your modeling time on Steps 1-2 (source navigation + grain confirmation), you'll spend the other 70% rewriting later. The trade is non-negotiable.

**Failure mode:** "I'll figure out the grain as I write it." Result: you write a fact table that double-counts because the upstream join produced a fan-out you didn't see.

### 2. `QUALIFY ROW_NUMBER() = 1` is a symptom, not a fix

Every time you reach for `QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) = 1` in a downstream model, ask: why isn't `x` the PK upstream? If the upstream's grain is correct, you don't need to dedupe.

**Failure mode:** stacking row-number filters in two or three layers, each one masking a different upstream grain bug. The model "works" but every change to upstream breaks downstream because the dedupes were load-bearing.

### 3. WHERE conditions disappear top-to-bottom

In a well-designed CTE chain, `WHERE` filters cluster at the top (in the source CTE or the first transform CTE). Each subsequent CTE narrows by aggregation, joining, or projection — but if you find yourself adding a `WHERE` in CTE #4, that's a signal you missed a filter at the top OR you're filtering after a join produced rows you should have excluded earlier.

**Failure mode:** a 500-line model where every CTE has a `WHERE` clause; nobody can reason about which rows survive to the end.

### 4. After every join, verify row count

Add a `COUNT(1)` debug query before and after every join in a new model. If the row count changes unexpectedly, the join key isn't unique and you have a fan-out. Catching this once saves an evening of head-scratching when the headline metric drifts.

**Failure mode:** dim's PK has duplicates because it's a snapshot dim with multiple SCD versions; fact joins to it; fact row count silently doubles; the headline metric reads 2x what it should and nobody notices for a week.

---

## When to query the corpus

Per `using-the-notebook.md`: query the Data Ops notebook (`scope: methodology.data_ops` or default `methodology`) before:

- Codifying a new modeling decision into a rule or a `_<dir>__docs.md` description
- Choosing between two materialization strategies for an intermediate model
- Picking a join pattern for a fact × dim where SCD Type 2 is in play
- Writing a parity test for a refactor

The Kim modeling article (this rule's source) is in the Data Ops notebook as a text source — `ask-corpus` with the right query will surface it directly.

---

## Lessons learned

- **2026-04-22 Speed-to-Lead F1/F2/F3 refactor (this engagement).** Collapsed 11 narrow `stl_*` rollup tables into one wide `speed_to_lead_detail` mart backed by `fct_speed_to_lead_touch`. Held a parity test green across the deprecation window before retiring the rollups. Pattern proves Kim's "fewer wider marts" maxim works in practice — and proves the parity-gated dual-source window is the safe deprecation pattern for any future refactor. (See `.claude/rules/mart-naming.md` lessons-learned + `docs/_archive/Davv5-Track-F[123]-*.md`.)
- *(Populate as new modeling lessons surface.)*
