---
title: "feat: ask-corpus → corpus research engine (last30days-parity Python pipeline)"
type: feat
status: active
date: 2026-04-26
deepened: 2026-04-26
origin: conversation on 2026-04-26 (no prior requirements doc; brief was given inline at /ce-plan invocation)
---

# Corpus Research Engine — `ask-corpus` v2

## Fresh session startup

If you're opening this plan in a new Claude session, do these in order **before touching anything**:

1. Read `CLAUDE.md` (repo conventions) and `.claude/state/project-state.md` for the current snapshot.
2. Confirm auto-loaded memory includes `feedback_multi_agent_orchestration.md` (supersedes `feedback_ship_over_ceremony.md` 2026-04-27) and `feedback_maximum_safe_speed.md`. Multi-agent pipelines are leverage when fanning out independent work; main-session execution remains the default for solo build steps.
3. Read this plan in full — especially **Key Technical Decisions**, **High-Level Technical Design**, and **Phased Delivery**.
4. Skim the upstream architecture this plan is modeled on: `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/scripts/lib/{pipeline,planner,fusion,rerank}.py`. The patterns ported here are taken from those files.
5. Confirm which unit you're executing. Default: the next unchecked `- [ ]` in Implementation Units order.

**Notebook before convention.** This plan creates the engine that enforces "query the notebook before scaffolding." When this plan's own units need a design call (e.g., "how should the rerank prompt be structured?"), it's exempt — we *are* the new query system. Use last30days as the reference instead.

---

## Overview

`ask-corpus` today is a thin SKILL.md wrapper that resolves a notebook ID from `.claude/corpus.yaml` and makes one `mcp__notebooklm-mcp__notebook_query` (or `cross_notebook_query`) call. One question in, one answer out — no planning, no fan-out, no rerank, no fusion, no diversity guards, no adversarial-content fencing, no traces.

This plan rebuilds it as a **planner-driven, fan-out-fuse-rerank Python research engine** modeled on `last30days-skill` v3.1.0's `pipeline.py`. The host LLM becomes the planner and the reranker (via JSON handshake — no external API key needed); the Python engine handles deterministic mechanics: fan-out across (subquery × scope) pairs, weighted RRF fusion with diversity caps, structured trace emission, and Report packaging.

The retrieval surface is `nlm` CLI subprocess (already installed at `/Users/david/.local/bin/nlm`). MCP tools remain available to the host LLM but are not the engine's primary path — `nlm` gives us subprocess-level control over rate limits, retries, traces, and test fixtures.

**Outcome:** querying the corpus matches last30days's rigor. Same architectural shape, same observability discipline, same documented-failure-mode anchors. Future rules and conventions are grounded in genuinely-merged-and-reranked citations across the four notebooks instead of a single retrieval pass.

---

## Problem Frame

`ask-corpus` v1 has known sharpness gaps that the inline `using-the-notebook.md` rule already documents:

- **Two distinct failure modes to address — they have different fixes (per adversarial review).**
  - **(A) Policy bypass — Claude reasons from first principles and skips the corpus entirely.** The 2026-04-19 mart-naming incident is the canonical example: Claude answered without querying; the user had to ask for a double-check; the corpus then surfaced three actionable specifics (separate by schema, drop `fct_/dim_` in marts, fewer wider marts over many narrow ones). The fix is **discipline + low friction**: keep the "always double-check" rule (codified as LAW 3 in v2 SKILL.md), and make invoking the engine *easier and faster than first-principles reasoning* so there's no incentive to skip. A heavier engine doesn't fix this — it can make it worse if invocation friction grows.
  - **(B) Adjacent-but-irrelevant citation — retrieval returns content "near" the right concept but not actually supporting the claim.** The Hermes Agent / Managed Agents class of failure (a video about a different but adjacent system scoring high). This is the failure mode the rerank step's primary-entity grounding penalty addresses. v2 has not yet observed this in this engagement; the rerank step is *structural insurance* against it, not a fix for an observed incident.
- **Authority asymmetry in cross-queries.** Data Ops has 50+ sources; Metabase Craft and Metabase Learn have fewer. In `cross_notebook_query`, the bigger notebook dominates retrieval — the smaller ones get drowned out for many questions. v2 needs a quality-aware diversity guard (count alone isn't enough; reserving slots for low-quality items can worsen answers — see Key Technical Decisions and U7).
- **No primary-entity grounding.** A citation can be "near" the right concept without actually supporting the claim. last30days's Hermes Agent Use Cases failure (a "Managed Agents" video that scored 51 with zero Hermes content) is the same failure mode that hits corpus retrieval when the notebook contains adjacent but irrelevant material.
- **No prompt-injection fence on retrieved content.** Notebooks ingest YouTube transcripts and web pages. Adversarial content in those sources currently flows un-fenced into any LLM step that reads citations.
- **Zero observability.** v1 returns an answer; v2 should emit `[Planner]` / `[Retriever]` / `[Fusion]` traces to stderr so retrieval-breadth failures are visible without a debug flag.
- **No structured Report.** v1 produces prose. v2 produces a structured `Report` (intent, plan, ranked candidates, clusters, warnings) that the host LLM synthesizes from — making the synthesis controllable via a voice contract.

**Why now (Phase A scope check):** this is methodology investment, not Gold-layer build. The Discovery Sprint pause forbids new dbt/warehouse/mart PRs; it does not forbid sharpening the agentic tooling that the next build phase will lean on. Per `feedback_maximum_safe_speed.md`, this passes the reversibility test (revert is one git revert away) and the priority test (sharpens every future rule and scaffolding decision).

---

## Requirements Trace

- **R1.** `ask-corpus` produces a structured `Report` (intent, plan, ranked candidates, clusters, warnings) instead of free-form prose. The SKILL.md voice contract synthesizes the Report into the user-facing answer.
- **R2.** Retrieval fans out across **(subquery × scope) pairs** in parallel. A default-scope query against `methodology` produces N subqueries × 3 notebooks = 3N retrieval calls before fusion.
- **R3.** Each subquery carries **two query forms**: `search_query` (keyword-style for `nlm` retrieval) and `ranking_query` (natural-language for the reranker prompt).
- **R4.** Fusion uses **weighted reciprocal rank fusion** (RRF, k=60) with per-scope diversity caps and a per-source-title cap to prevent any single document from dominating the merged candidate pool.
- **R5.** Reranking is performed by the **host LLM** via a JSON handshake. `--phase=retrieve` emits the rerank prompt to disk; the host scores; `--phase=finalize --rerank-scores ...` consumes the scores. The rerank prompt fences candidate content in `<untrusted_content>` tags, includes intent-specific scoring hints, and applies a primary-entity grounding penalty.
- **R6.** **Phase-2 entity supplemental search:** after Phase-1 retrieval, extract repeated source titles and notable phrases from the citation set, run targeted follow-up `nlm` queries against the same scopes at lower weight (0.3).
- **R7.** **Phase-2b thin-scope retry:** scopes that returned <3 useful citations get retried with a simplified `extract_core_subject(topic, max_words=3)` query at weight 0.3.
- **R8.** **Always-on stderr traces:** `[Planner]` summary line + per-subquery line, `[Retriever]` per-stream completion line, `[Fusion]` candidate-pool summary, `[Rerank]` shortlist summary, `[Cluster]` final-cluster summary. No debug flag required.
- **R9.** **Structured warnings:** thin-evidence (<5 candidates), source-concentration (top 5 from one scope), scope-errors (any scope failed retrieval), no-usable-items.
- **R10.** **Documented failure-mode anchors (LAWs)** in SKILL.md tied to specific incidents (the 2026-04-19 mart-naming directional-but-incomplete case carried over; new incidents added as they surface).
- **R11.** **No new external API keys required.** Planner and reranker LLM calls are made by the host LLM via the JSON handshake. `nlm` CLI provides retrieval. The skill remains free at the marginal call.
- **R12.** **`corpus.yaml` shape preserved.** No schema migration; v2 reads the existing structure unchanged. Optional fields (e.g., per-scope retrieval weight, per-scope size hint) are additive and default safely when absent.
- **R13.** **Test suite** with pytest, mirroring last30days's discipline: planner sanitizer tests, fusion math tests, dedup tests, rerank-prompt structure tests, and a smoke test that runs the full pipeline with captured `nlm` fixture responses.

---

## Scope Boundaries

- **No new external LLM provider plumbing.** `last30days` has `providers.py` (OpenAI / xAI / OpenRouter HTTP clients). v2 deliberately omits this; the host LLM is the only LLM. If headless/cron use ever matters, that becomes a follow-up.
- **No backend cascade per source.** `last30days` has Reddit-public→ScrapeCreators, YouTube-yt-dlp→ScrapeCreators, X-bird→xai→xurl. We have one retrieval surface (`nlm`) and one fallback shape (single-notebook `nlm notebook query` ↔ multi-notebook `nlm cross`). No further cascade.
- **No per-author cap.** Notebook citations don't carry "authors" the way social-media posts do. The diversity unit is the **source title** (e.g., a YouTube video, a doc, a transcript) — capped instead.
- **No GitHub star enrichment / Studio polling / Phase-3 enrichment passes.** Those are last30days surfaces unrelated to corpus retrieval.
- **No rate-limit shared-signal infrastructure.** `nlm` CLI is local subprocess; the underlying NotebookLM API can throttle but per-call we treat each as independent. Transient errors get a single retry; rate-limit responses are surfaced as warnings.
- **No SKILL.md hard versioning / canonical-path self-check (yet).** last30days' STEP 0 stale-marketplace-clone defense exists because that skill ships via the public marketplace. ask-corpus is repo-local. Defer until/unless we publish externally.
- **No Pinterest / Bluesky / Threads / Polymarket equivalents.** Single retrieval domain (notebooks).
- **No deletion of MCP-based path.** The `mcp__notebooklm-mcp__*` tools remain callable by the host LLM directly for ad-hoc queries outside this skill. v2 owns the orchestrated path; raw MCP stays available.

### Deferred to Follow-Up Work

- **External LLM provider plumbing** (`providers.py` shape) — when/if we want headless cron-style runs.
- **Studio-artifact integration** — generating audio/slide briefings from a Report (separate skill, not this one).
- **Vendor-API corpus expansion** — already parked in `project_vendor_api_corpus_deferred.md`; this engine will *consume* those notebooks once they exist, not build them.
- **Cross-engagement portability** — current engagement notebook is D-DEE-specific; a future skill version may reload `corpus.yaml` per-engagement, but that's not in v2's scope.
- **Programmatic corpus.yaml validation** (e.g., on PostToolUse) — the auto-sync hook already covers rule files; a parallel hook to validate `corpus.yaml` shape is a separate piece of work.

---

## Context & Research

### Relevant Code and Patterns

**Upstream reference (model for the architecture):**
- `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/scripts/lib/pipeline.py` — orchestration + Phase 1 / Phase 2 / Phase 2b
- `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/scripts/lib/planner.py` — prompt rules, sanitizer, deterministic fallback, `SOURCE_CAPABILITIES` mapping
- `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/scripts/lib/fusion.py` — weighted RRF, per-author cap, diversity pool
- `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/scripts/lib/rerank.py` — `UNTRUSTED_CONTENT` fence, intent hints, primary-entity grounding penalty
- `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/SKILL.md` — voice contract, LAW system, badge anchor, post-synthesis self-check
- `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/tests/` — test patterns to mirror

**Repo-side (current state):**
- `.claude/skills/ask-corpus/SKILL.md` — v1 implementation; replaced by v2 SKILL.md in U13
- `.claude/corpus.yaml` — notebook IDs and purposes; preserved unchanged
- `.claude/rules/using-the-notebook.md` — routing rule; updated in U15 to point at v2's `--plan` handshake and document the new contract
- `.claude/scripts/sync-rule-to-notebook.sh` — PostToolUse hook; unchanged but referenced
- `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — formatting + voice precedent; this plan follows the same structure (frontmatter, Fresh session startup, phased delivery with HARD GATEs noted only where parity matters)

**External dependency already installed:**
- `nlm` CLI at `/Users/david/.local/bin/nlm` — `notebook query`, `cross`, `notebook describe` cover the retrieval surface

### Institutional Learnings

`docs/solutions/` does not yet exist in this repo. Learnings to capture as the engine ships:
- The 2026-04-19 mart-naming incident → first inline LAW anchor in v2 SKILL.md.
- Authority asymmetry in `cross_notebook_query` → drives the per-scope diversity cap design.
- The "ceremony substitution" failure mode (citing the corpus as ritual without sharpening) → drives the warnings emitter.

### External References

- Reciprocal rank fusion: Cormack et al. 2009. RRF_K=60 is the standard smoothing constant — used in last30days/fusion.py:9 and inherited here unchanged.
- NotebookLM API behavior: `notebook_query` and `cross_notebook_query` are free at the marginal call per `using-the-notebook.md`; assume that holds.

---

## Key Technical Decisions

- **Python engine + JSON-handshake LLM steps. The two-phase handshake is novel, not inherited.** last30days uses `--plan` once at invocation (host-as-planner) and runs rerank engine-internal via `providers.py`. v2 makes the host LLM both planner *and* reranker via a `--phase=retrieve` → host scores → `--phase=finalize` round-trip. This sidesteps "which API key" but is a net-new architectural pattern with no upstream battle-testing — the riskiest novel design choice in this plan. Mitigations are explicitly required (see U8 schema validation + self-contained rerank prompt + mock-LLM test fixture). Do not present this as parity in code comments or docs.
- **`nlm notebook query --json` is the only retrieval primitive. No `nlm cross` path.** Verified at plan-revision time: `nlm cross query` does not accept `--json` (returns "No such option"); only `nlm notebook query --json` produces structured output. Default-scope queries that hit 3 methodology notebooks fan out as 3 parallel `nlm notebook query --json` calls — the engine's own ThreadPoolExecutor produces the cross-query effect with structured output. The MCP `cross_notebook_query` tool stays available for ad-hoc host-LLM use but is not the engine's path.
- **`nlm notebook query --json` returns one synthesized answer per query — NOT a ranked list of items.** Verified at U3 implementation time, the actual response shape is `{value: {answer: "<markdown>", conversation_id: ..., sources_used: [uuid, ...], citations: {"1": uuid, "2": uuid, ...}}}`. The `answer` is a markdown string with inline `[N, M]` citation markers; `citations` maps citation numbers → source UUIDs. **There is no `cited_text` field per source.** This makes corpus retrieval *richer* than last30days's social-media retrieval (we get LLM-curated context per source) but requires us to extract per-citation snippets from the answer text. Approach: for each unique `source_id` referenced in the answer, the snippet is the sentence(s) containing the citation marker(s) that map to that source. Implemented in `retriever.py` (U3).
- **`nlm source list --json` returns `[{id, title, type, url}, ...]` — title resolution is straightforward.** Per-source-id diversity cap operates on `source_id` directly (max 3 hits per source_id in the merged pool). Source titles are fetched lazily via `nlm source list --json <notebook_id>` *only for candidates that survive rerank*, with per-process caching keyed by notebook_id. Lazy fetch keeps the hot retrieval path single-API-call per (subquery × scope).
- **Scope, not source.** last30days's "source" abstraction (reddit/x/youtube/etc.) maps to our **scope** (`methodology.data_ops`, `methodology.metabase`, `methodology.metabase_learn`, `engagement`). The scope is the per-stream key everywhere — capabilities, weights, diversity guards.
- **`corpus.yaml` schema preserved; optional `weight` and `size_hint` keys are additive.** v2 does not require existing entries to change. If `weight` is absent, default to 1.0. If `size_hint` is absent, the "authority asymmetry" guard still works via the diversity-cap heuristic; `size_hint` just tunes it.
- **No per-author cap. Per-source-id cap (max 3) instead.** Notebook citations carry a `source_id` (stable per source document); cap at 3 stops a single long transcript from monopolizing the merged pool. Titles are fetched lazily only for the post-rerank candidates (see decision above).
- **Quality-aware diversity guard, not pure-count diversity guard.** Per scope-guardian + adversarial review: pure count-based reservation (every above-threshold scope gets ≥2 reserved slots) can *inject mediocre citations* if a scope's best item barely clears threshold while the dominant scope's later items are far stronger. Conditional reservation: a scope gets reserved slots only when `(scope_top_relevance / dominant_scope_top_relevance) >= 0.6` (quality-parity floor). Below that ratio, the scope competes on RRF merit alone — no slot reservation. Stops the guard from worsening answers on questions where one notebook is the genuine authority.
- **Diversity guard threshold = 0.30 local relevance, but the threshold is provisional.** Both the threshold (0.30) and the local-relevance scoring function it operates on are unverified at plan time. Approach: ship with the threshold as a named constant in `fusion.py`, ship a deliberately-simple `local_relevance` scorer (snippet/ranking_query term overlap) in `signals.py`, and define an explicit acceptance criterion before locking the threshold: pick 3 questions with known-correct-scope answers, run the engine, confirm the diversity guard puts at least one citation from the correct scope in the top-5. Tune the threshold (or replace the scorer) until that holds. Both `local_relevance` and the threshold are explicitly Phase-2 tuning surfaces, not "iterate later."
- **Phase-2 entity extraction adapted: extract repeated source titles + named entities from `ranking_query`.** No "X handles" / "subreddits" equivalent. Repeated source titles indicate a strong document; named entities indicate a domain to drill into.
- **LAW system scoped down at launch.** Three LAWs only at v2.0 — earned, not cargo-culted: (1) no `Sources:` block at the end (architecture-driven; engine emits citations inline), (2) engine footer pass-through (mechanical), (3) always-double-check the corpus before locking advice into a rule (the 2026-04-19 mart-naming anchor — the canonical incident this whole engine exists to address). Formatting preferences (no em-dashes, no `##` body headers, no invented title) live in the synthesis voice template at v2.0, not as LAWs. Promote to LAW status only when an incident in this skill produces the failure mode. Documented per-incident LAW growth is what makes last30days's anchors load-bearing; copying the structure without the incidents would be ceremony.
- **Synthesis voice contract adapted to corpus output.** Default body shape: `What the sources say:` prose label + bold-lead-in paragraphs grouped by claim, then `KEY PATTERNS from the corpus:` numbered list, then engine footer (citations + warnings), then invitation. No section headers in body.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

### Data flow (two-phase host-LLM handshake)

```mermaid
sequenceDiagram
    autonumber
    participant H as Host LLM (Claude)
    participant E as corpus_research.py
    participant N as nlm CLI subprocess
    participant L as corpus_lib (planner/fusion/rerank/cluster)

    H->>H: read user question, read SKILL.md voice contract
    H->>H: generate plan JSON (intent, subqueries, scope_weights)
    H->>E: --phase=retrieve --plan plan.json --question "..."
    E->>L: planner._sanitize_plan(plan)
    par fan-out (subquery × scope)
        E->>N: nlm notebook query <id> "<search_query>"
    and
        E->>N: nlm cross --ids ... "<search_query>"
    end
    L->>L: normalize → signals → dedupe per stream
    L->>L: Phase 2 entity supplemental
    L->>L: Phase 2b thin-scope retry
    L->>L: weighted RRF + diversity caps
    E-->>H: writes shortlist.json + rerank_prompt.md to /tmp; emits paths
    H->>H: read rerank_prompt.md, score candidates as JSON
    H->>E: --phase=finalize --shortlist shortlist.json --rerank-scores scores.json
    E->>L: rerank.apply_scores → cluster.cluster_candidates → schema.Report
    E-->>H: writes report.json to /tmp; emits path
    H->>H: synthesize per voice contract using report.json
```

### Subquery shape (the load-bearing primitive)

```
SubQuery {
  label:          "primary"        // short stable id; RRF weight key
  search_query:   "star schema dbt"  // keyword-style; passed to nlm
  ranking_query:  "What does the corpus say about how to model star schemas in dbt?"  // natural language; passed to reranker
  scopes:         ["methodology.data_ops", "methodology.metabase_learn"]
  weight:         1.0
}
```

The two-query split is the highest-leverage pattern from last30days: keyword retrieval on `search_query`, semantic relevance scoring on `ranking_query`. Conflating them is the most common failure mode.

### Scope-capability mapping (drives planner scope selection)

```
SCOPE_CAPABILITIES = {
  "methodology.data_ops":       {craft, dbt, modeling, ci_cd, mds_theory},
  "methodology.metabase":       {metabase_ops, metabase_integration, licensing, bigquery_cost},
  "methodology.metabase_learn": {metabase_authoring, dashboards, sql_howto, viz_choice, bi_transition},
  "engagement":                 {client_decisions, oracle_metrics, scope_history},
}

INTENT_DEFAULT_SCOPES = {
  "convention":     [data_ops, metabase, metabase_learn]   // cross-craft default
  "ops":            [metabase, data_ops]
  "howto":          [metabase_learn, data_ops]
  "history":        [engagement]
  "design":         [data_ops, metabase, metabase_learn]
}
```

### Output shape (synthesis input, what the host LLM consumes)

```
Report {
  question:           "..."
  intent:             "convention" | "ops" | "howto" | "history" | "design"
  plan:               { subqueries: [...], scope_weights: {...} }
  ranked_candidates:  [Candidate { source_id, source_title, scope, snippet, citation_number, score, ranking_query }, ...]
                       // source_title is filled lazily after rerank via cached `nlm source list --json`
                       // source_id is the stable join key from `nlm notebook query --json`
  clusters:           [Cluster { theme, candidates: [...] }, ...]    // grouped inline by subquery label; no separate cluster module at v2.0
  warnings:           ["thin-evidence" | "scope-concentration" | "scope-errors" | "no-usable-items" | "plan-fallback" | "rerank-fallback"]
  trace_summary:      { plan_source: "host-llm" | "deterministic", n_subqueries, n_streams_run, n_streams_errored }
}
```

---

## Output Structure

```
.claude/skills/ask-corpus/
  SKILL.md                          # rewritten in U13: voice contract + LAWs + handshake protocol
  scripts/
    corpus_research.py              # entry point; argparse; two phases (retrieve / finalize)
    pyproject.toml                  # pytest config + ruff lint
    corpus_lib/
      __init__.py
      schema.py                     # SubQuery, QueryPlan, SourceItem, Candidate, Cluster, Report dataclasses
      env.py                        # corpus.yaml resolution + scope ↔ notebook_id mapping
      retriever.py                  # nlm CLI subprocess wrappers (single-notebook + cross); transient retry; trace emission
      planner.py                    # _sanitize_plan; deterministic fallback; SCOPE_CAPABILITIES; INTENT_DEFAULT_SCOPES
      normalize.py                  # raw nlm JSON → SourceItem; canonical shape
      signals.py                    # local relevance, freshness (corpus-context: source recency from notebook_describe), source quality
      dedupe.py                     # within-stream dedup by (source_title, snippet-hash)
      fusion.py                     # weighted_rrf; per-source-title cap; per-scope diversity guard
      rerank.py                     # _build_prompt with UNTRUSTED_CONTENT fence + intent hints + grounding penalty; apply_scores
      cluster.py                    # cluster by ranking_query overlap
      entity_extract.py             # repeated source_titles + named-entity extraction from Phase 1
      query.py                      # extract_core_subject(topic, max_words=3) for thin-scope retry
      log.py                        # stderr trace formatters: [Planner], [Retriever], [Fusion], [Rerank], [Cluster]
    fixtures/
      nlm_data_ops_star_schema.json # captured nlm response for tests
      nlm_metabase_backup.json
      ...
  tests/
    test_planner.py                 # sanitizer + deterministic fallback + intent inference
    test_fusion.py                  # weighted RRF math + diversity caps + per-source-title cap
    test_dedupe.py                  # snippet-hash dedup
    test_rerank_prompt.py           # UNTRUSTED_CONTENT fence + grounding clause + scoring scale
    test_pipeline_smoke.py          # full pipeline against fixtures; asserts Report shape
    test_env.py                     # corpus.yaml resolution + safe defaults
.claude/rules/
  using-the-notebook.md             # updated in U15: handshake protocol, when to invoke v2
docs/plans/
  2026-04-26-001-feat-corpus-research-engine-plan.md   # this file
```

---

## Implementation Units

### Phase 1 — Skeleton end-to-end (single subquery, single scope, no rerank yet)

Goal: prove the subprocess + JSON handshake plumbing before adding sophistication.

- [x] **U1. Repo scaffolding + dataclasses (`schema.py`)** *(landed 2026-04-26; 12/12 unittest pass)*

  **Goal:** create the directory layout under `.claude/skills/ask-corpus/scripts/`, define `schema.py` so subsequent units have stable types to import. Inherit pytest + ruff config from the **existing root `pyproject.toml`** — do not nest a second pyproject.

  **Requirements:** R1, R12.

  **Dependencies:** None.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/__init__.py`
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/schema.py`
  - Create: `.claude/skills/ask-corpus/tests/__init__.py`
  - Create: `.claude/skills/ask-corpus/tests/test_schema.py`
  - Modify (if needed): root `pyproject.toml` — add `.claude/skills/ask-corpus/tests` to pytest discovery; remove `.claude/skills/ask-corpus/scripts` from any ruff `extend-exclude` if listed.

  **Approach:**
  - Dataclasses (frozen where stable, mutable where mutated by the pipeline): `SubQuery`, `QueryPlan`, `SourceItem`, `Candidate`, `Cluster`, `Report`, `RetrievalBundle`.
  - Match last30days's field set where applicable; rename `source` → `scope`, drop `engagement` field (the metric, not the notebook), `native_ranks` (no per-rank tracking yet — added in U5 if RRF needs it).
  - **No nested pyproject.toml** — the root `pyproject.toml` already configures ruff (E, F, I, W, B, UP, SIM rules; py311 target) and is sufficient. Tests run as `pytest .claude/skills/ask-corpus/tests` from repo root.

  **Patterns to follow:**
  - `last30days/scripts/lib/schema.py` (assumed location based on imports in pipeline.py:34) — mirror its dataclass discipline.

  **Test scenarios:**
  - Happy path: `SubQuery(label="primary", search_query="x", ranking_query="y", scopes=["methodology.data_ops"], weight=1.0)` constructs and round-trips through `dataclasses.asdict`.
  - Edge case: `Report` with empty `ranked_candidates` and no warnings still serializes to JSON cleanly.

  **Verification:**
  - `pytest -q .claude/skills/ask-corpus/tests/test_schema.py` passes from repo root.

- [x] **U2. `env.py` — `corpus.yaml` resolution + scope helpers** *(landed 2026-04-26; 15 env tests + live corpus.yaml smoke; 27/27 cumulative pass)*

  **Goal:** central scope-resolution helper used by every downstream module.

  **Requirements:** R12.

  **Dependencies:** U1.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/env.py`
  - Create: `.claude/skills/ask-corpus/tests/test_env.py`
  - Create: `.claude/skills/ask-corpus/tests/fixtures/corpus_minimal.yaml`

  **Approach:**
  - One public function: `resolve_scopes(scope: str | None, config_path: Path) -> list[ScopeRef]` where `ScopeRef = (key, notebook_id, name, weight, size_hint)`.
  - Replicates the existing inline bash/python in v1 SKILL.md but as a tested library function instead of inline-script-by-prompt.
  - Defaults: scope `None` or `"methodology"` → all methodology entries. `"engagement"` → engagement entry. `"methodology.data_ops"` → exactly that one. Unknown key → raises `UnknownScopeError`.
  - Reads optional `weight` and `size_hint` keys; defaults 1.0 / `None`.

  **Patterns to follow:**
  - The v1 SKILL.md inline snippet (`.claude/skills/ask-corpus/SKILL.md:38-78`) defines the resolution semantics — keep them identical.

  **Test scenarios:**
  - Happy path: scope `"methodology.data_ops"` returns one `ScopeRef` with the Data Ops UUID.
  - Happy path: scope `None` and `"methodology"` both return all three methodology entries in declaration order.
  - Edge case: missing `corpus.yaml` returns the documented hardcoded fallback (Data Ops UUID) and emits one stderr warning.
  - Error path: scope `"methodology.bogus"` raises `UnknownScopeError`.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_env.py -q` passes.

- [x] **U3. `retriever.py` — `nlm` CLI subprocess wrappers** *(landed 2026-04-26; 13 tests; real fixture captured; major discovery: nlm answer is markdown with inline `[N, M]` and `[N-M]` citations, plan revised to match)*

  **Goal:** isolate every shell-out to `nlm` behind two functions; emit `[Retriever]` traces; handle transient errors.

  **Requirements:** R8.

  **Dependencies:** U1, U2.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/retriever.py`
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/log.py`
  - Create: `.claude/skills/ask-corpus/tests/test_retriever.py`
  - Create: `.claude/skills/ask-corpus/tests/fixtures/nlm_query_data_ops_minimal.json`

  **Approach:**
  - **One retrieval primitive only:** `query_one(notebook_id, search_query, *, timeout=30) -> RawHit[]` wraps `nlm notebook query --json <id> "<search_query>"`. Returns parsed JSON `references[]` with `{source_id, citation_number, cited_text}` per hit. **No `query_cross` function** — verified at plan time that `nlm cross query --json` is not supported; cross-scope retrieval is achieved by parallel `query_one` calls in `pipeline.py` (U5).
  - `list_sources(notebook_id) -> dict[source_id, source_title]` wraps `nlm source list --json <notebook_id>`. **Called lazily** only after rerank, only for the surviving candidates' `source_id`s, with per-process caching keyed by `notebook_id`. Title resolution for synthesis only — never on the hot retrieval path.
  - Both functions: check exit code, retry once on transient failure (5xx-style exit), emit `[Retriever] scope=<name> hits=<N> took=<ms>` and `[SourceList] scope=<name> n=<N> cached=<bool>` to stderr.
  - `log.py` exposes `trace(component: str, msg: str)` writing to stderr with consistent formatting.

  **Patterns to follow:**
  - `last30days/scripts/lib/pipeline.py:_retrieve_stream` for the per-source dispatch and retry shape. Adapt to single-source (nlm) — drop the cascade.
  - Trace style: `last30days/scripts/lib/pipeline.py:242-257` (always-on planner trace).

  **Test scenarios:**
  - Happy path: `query_one(uuid, "star schema")` against a stubbed subprocess returns parsed `RawHit[]` with `{source_id, citation_number, cited_text}` keys.
  - Happy path: `list_sources(uuid)` against a stubbed subprocess returns dict; second call within process returns cached value; trace shows `cached=true`.
  - Error path: subprocess returns nonzero; first call fails, retry succeeds, return value matches retry result; `[Retriever]` trace records both attempts.
  - Error path: both attempts fail; raises `RetrievalError` with scope context.
  - Edge case: empty `nlm` output → returns `[]`, not error.

  **Execution note:** `nlm notebook query --json` and `nlm source list --json` were verified at plan-revision time as the supported forms. `nlm cross query --json` is **not** supported — do not introduce a `query_cross` wrapper.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_retriever.py -q` passes.
  - Manual smoke: `python3 -c "from corpus_lib.retriever import query_one; print(len(query_one('7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a', 'star schema')))"` returns >0.

---

### Phase 2 — Pipeline core (planner + fan-out + fusion + rerank)

Goal: from skeleton to "the rerank fence and primary-entity grounding work end-to-end."

- [x] **U4. `planner.py` — sanitizer + deterministic fallback + capabilities + INTENT_DEFAULT_SCOPES** *(landed 2026-04-26; 23 planner tests; 63/63 cumulative pass)*

  **Goal:** accept either a host-LLM-supplied plan JSON or fall back to deterministic plan generation. Validate, normalize, ensure every scope is reachable.

  **Requirements:** R3, R6, R11.

  **Dependencies:** U1, U2.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/planner.py`
  - Create: `.claude/skills/ask-corpus/tests/test_planner.py`

  **Approach:**
  - Public surface: `plan_query(*, question, scopes, supplied_plan: dict | None) -> QueryPlan`.
  - If `supplied_plan` present: `_sanitize_plan` — validate intent in allowed set, validate every subquery has both `search_query` and `ranking_query`, validate scopes are in available list, normalize weights.
  - If absent: `_fallback_plan(question, scopes)` — emit a single `SubQuery(label="primary", search_query=question, ranking_query=question, scopes=scopes, weight=1.0)` plus a stderr `[Planner] No plan supplied; using deterministic single-subquery fallback. The host LLM should generate a plan and pass it via --plan for richer retrieval.` line.
  - `SCOPE_CAPABILITIES` and `INTENT_DEFAULT_SCOPES` constants (per High-Level Technical Design).
  - Allowed intents: `convention`, `ops`, `howto`, `history`, `design`. Subset of last30days's intents — corpus-domain-specific. Documented inline.
  - **Planner prompt is in SKILL.md, not planner.py.** The host LLM reads it from SKILL.md (U13). planner.py only consumes the resulting JSON.

  **Patterns to follow:**
  - `last30days/scripts/lib/planner.py:_sanitize_plan` and `_fallback_plan`.
  - `last30days/scripts/lib/planner.py:118-143` for the "host LLM IS the planner" stderr nudge.

  **Test scenarios:**
  - Happy path: well-formed plan JSON sanitizes to a `QueryPlan` with same subqueries and normalized weights.
  - Edge case: plan with one subquery missing `ranking_query` → that subquery is dropped, others kept, warning logged.
  - Edge case: plan with intent `"frobnicate"` (not in allowed set) → intent reset to `_infer_intent(question)`, warning logged.
  - Error path: empty plan with no fallback path → `_fallback_plan` returns single-subquery plan and emits the host-LLM-nudge trace.
  - Edge case: scope `"methodology.bogus"` in supplied plan is filtered out; if no scopes remain, falls back to default scopes for the inferred intent.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_planner.py -q` passes.

- [x] **U5. `pipeline.py` first cut — fan-out + per-stream normalize/signals/dedupe + traces** *(landed 2026-04-26; 6 dedupe + 5 pipeline-smoke tests; 74/74 cumulative pass)*

  **Goal:** the orchestrator runs end-to-end against real `nlm` and produces a flat candidate list. No fusion or rerank yet — those come in U7/U8.

  **Requirements:** R2, R8, R9.

  **Dependencies:** U3, U4.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/pipeline.py`
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/normalize.py`
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/signals.py`
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/dedupe.py`
  - Create: `.claude/skills/ask-corpus/tests/test_pipeline_smoke.py`
  - Create: `.claude/skills/ask-corpus/tests/test_dedupe.py`

  **Approach:**
  - `pipeline.run(*, question, scopes, supplied_plan, depth)` — the public entry point.
  - `ThreadPoolExecutor(max_workers=3)` fanning out (subquery × scope) to `retriever.query_one`. **Capped at 3** per feasibility review — NotebookLM API rate-limits apply per account, and a default-scope query already issues 3 (subqueries) × 3 (notebooks) = 9 calls before any supplemental work. Higher concurrency risks throttling without measured headroom.
  - **No cross-query path.** Default-scope queries ("methodology") fan out as one `query_one` call per (subquery × notebook), serialized by the executor's worker pool.
  - Per-stream pipeline: `normalize.normalize_hits → signals.annotate_stream → signals.prune_low_relevance → dedupe.dedupe_within_stream`.
  - `normalize.py`: raw nlm JSON `{source_id, citation_number, cited_text}` → `SourceItem(source_id, scope, snippet=cited_text, citation_number, raw_relevance=None, ...)`. **No source_title in the SourceItem at this stage** — title is filled lazily after rerank.
  - `signals.py`: `local_relevance` is a snippet/ranking_query term-overlap function. **This is a load-bearing tuning surface, not "iterate later":** the diversity guard threshold (U7) and prune threshold both depend on it. Ship a deliberately-simple scorer (case-folded token Jaccard with stopword removal); pin tunable constants in `signals.py` (overlap threshold, stopword set); revisit before declaring U7 done. `source_quality` per scope from `corpus.yaml` weight (default 1.0). Freshness omitted at v2.0 (corpus content is static).
  - `dedupe.py`: dedup by `(source_id, snippet[:120])` hash within stream.
  - Emit per-stream `[Retriever]` trace + summary `[Pipeline] streams_run=N errored=M items_kept=K`.

  **Patterns to follow:**
  - `last30days/scripts/lib/pipeline.py:run` overall shape (lines 163-487).
  - `last30days/scripts/lib/pipeline.py:_normalize_score_dedupe` per-stream pipeline.

  **Test scenarios:**
  - Happy path (smoke): full pipeline against fixture nlm responses for 1 subquery × 2 scopes returns a flat candidate list with traces emitted.
  - Edge case: one of the two scopes errors; pipeline succeeds with `errors_by_scope` populated and a `scope-errors` warning queued.
  - Edge case: all scopes return empty → `no-usable-items` warning; `ranked_candidates=[]`.
  - Dedupe unit test: two hits with same `(source_title, snippet[:120])` collapse to one.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_pipeline_smoke.py -q` passes.
  - Stderr trace shows `[Planner] ... source=deterministic` and `[Retriever] scope=methodology.data_ops n=N took=Xms` lines.

- [x] **U6. `entry point` — `corpus_research.py` argparse + two phases** *(landed 2026-04-26; 7 CLI tests covering retrieve→finalize round-trip, exit codes 0/2/4, fallback warning emission; landed out of order after U7+U8 to allow finalize phase to wire rerank.apply_scores; 116/116 cumulative pass)*

  **Goal:** wire the CLI surface that the host LLM calls with `--phase=retrieve` and `--phase=finalize`.

  **Requirements:** R11.

  **Dependencies:** U5.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_research.py`

  **Approach:**
  - Argparse: `--phase {retrieve, finalize}`, `--question`, `--scope`, `--plan` (path), `--shortlist` (path; phase=finalize only), `--rerank-scores` (path; phase=finalize only), `--out-dir` (defaults to `$TMPDIR/ask-corpus-<rand>`), `--depth {quick,default,deep}` (default `default`).
  - `--phase=retrieve`: invoke `pipeline.run`, write `shortlist.json` and `rerank_prompt.md` to `out-dir`, print the two paths to stdout for the host LLM to read.
  - `--phase=finalize`: load `shortlist.json` + `rerank-scores`, call `rerank.apply_scores` → `cluster.cluster_candidates` → package `Report`, write `report.json` to `out-dir`, print path to stdout.
  - Exit codes: 0 on success, 2 on usage error, 3 on retrieval failure, 4 on plan validation failure.

  **Patterns to follow:**
  - `last30days/scripts/last30days.py` for argparse layout (verify path under `scripts/`).

  **Test scenarios:**
  - Happy path: `corpus_research.py --phase=retrieve --question "star schema in dbt" --scope methodology` writes a shortlist + rerank-prompt and prints both paths.
  - Edge case: `--phase=finalize` without `--shortlist` exits 2 with a clear message.
  - Edge case: malformed `--plan` JSON exits 4 and emits `[Planner]` validation trace.

  **Verification:**
  - Manual smoke (after U7/U8 land too): full two-phase round-trip against the live Data Ops notebook returns a Report.

- [x] **U7. `fusion.py` — weighted RRF + diversity caps** *(landed 2026-04-26; 9 fusion tests; per-source-id collapse via `candidate_id == source_id`; quality-aware diversity guard validated; 83/83 cumulative pass)*

  **Goal:** merge per-(subquery, scope) streams into one ranked candidate pool with diversity guards.

  **Requirements:** R4.

  **Dependencies:** U5.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/fusion.py`
  - Create: `.claude/skills/ask-corpus/tests/test_fusion.py`

  **Approach:**
  - `weighted_rrf(streams: dict[(label, scope), list[SourceItem]], plan: QueryPlan, *, pool_limit: int) -> list[Candidate]`.
  - Score: `(subquery.weight × scope_weights[scope]) / (RRF_K + native_rank)` summed across streams a candidate appears in. `RRF_K=60`. Candidate key is `source_id` (stable per source document across scopes).
  - **Per-source-id cap (max 3):** no single source document (one `source_id`) appears more than 3 times in the merged pool.
  - **Quality-aware per-scope diversity guard:** a scope reserves ≥2 slots in the pool only when *both* (a) its best item's `local_relevance >= DIVERSITY_THRESHOLD` (constant in module, ship-default 0.30, **provisional**), AND (b) `scope_top_relevance / dominant_scope_top_relevance >= QUALITY_PARITY_FLOOR` (constant, ship-default 0.6). Below the parity floor, the scope competes on RRF merit alone — no slot reservation. This is the adversarial-review fix: stops the guard from injecting mediocre citations when a scope is nominally above threshold but materially weaker than the dominant scope for *this* question.
  - Sort key: `(-rrf_score, -local_relevance, scope_label, source_id)`.

  **Patterns to follow:**
  - `last30days/scripts/lib/fusion.py:weighted_rrf` line by line. Drop `_apply_per_author_cap`; replace with per-source-title cap. Keep `_diversify_pool` shape; rename `min_per_source` → `min_per_scope`.

  **Test scenarios:**
  - Happy path: candidate appearing in 2 streams accumulates score from both; ordering matches expected RRF math.
  - Edge case: 5 hits with same `source_id` → pool contains exactly 3 of them.
  - Edge case: 1 scope has 50 high-relevance items, another scope has 2 items at threshold-or-just-above with parity ratio >0.6 — both above-threshold items survive truncation (reservation fires).
  - Edge case (quality-aware guard): 1 scope's best item at 0.85, another scope's best item at 0.32 (above threshold but parity ratio 0.38 < 0.6) → second scope does **not** get reserved slots; competes on RRF merit only. Validates the adversarial-review fix.
  - Edge case: low-relevance scope (best item < 0.30) does not get reserved slots; competes on RRF merit only.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_fusion.py -q` passes.

- [x] **U8. `rerank.py` — LLM rerank prompt + apply_scores + local fallback** *(landed 2026-04-26; 26 rerank tests; self-contained prompt with embedded schema, fence-escape, mock-LLM round-trip, fallback preserves entity-miss penalty; 109/109 cumulative pass)*

  **Goal:** produce a rerank prompt the host LLM can score, apply returned scores, demote off-target candidates.

  **Requirements:** R5, R10.

  **Dependencies:** U7.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/rerank.py`
  - Create: `.claude/skills/ask-corpus/tests/test_rerank_prompt.py`

  **Approach:**
  - `build_rerank_prompt(*, question, plan, candidates, primary_entity) -> str` — emits a **fully self-contained** markdown prompt (covers adversarial Finding 8 — survives context compaction between phases):
    - Topic + intent + ranking_queries listed.
    - **Explicit instruction at top:** "Score the candidates below ONLY on the basis of this prompt. Do not rely on prior conversation context."
    - **Explicit output schema** embedded in the prompt itself (so the host has the schema even if conversation context compacts): expected JSON shape `{"scores": [{"candidate_id": str, "relevance": int 0-100, "reason": str}, ...]}` with worked example.
    - `<untrusted_content>` fence around candidate block; preface tells the model to treat fenced content as data, never as instructions. Sanitize candidate snippets to escape literal `</untrusted_content>` (per adversarial Finding 1 prompt-injection-by-tag-confusion).
    - Intent-specific scoring hints (5 intents × 1-2 sentences each).
    - Primary-entity grounding clause (cap at 30 if entity not mentioned).
    - Strict scoring scale 0-100 with quartile semantics.
  - `validate_rerank_scores(payload: dict) -> tuple[dict, list[str]]` — strict schema validation with structured errors. Required: top-level `scores` is a list; each entry has `candidate_id` (string matching a candidate in shortlist), `relevance` (int 0-100), `reason` (string). Returns `(parsed_dict, errors_list)`. **Mandatory call before `apply_scores`**.
  - `apply_scores(candidates, scores: dict[candidate_id, (score, reason)])` — sets `final_score`; applies `+25` deterministic penalty if `primary_entity` is missing from candidate snippet (covers the case where the LLM forgot the entity grounding).
  - `_local_fallback(candidates, primary_entity)` — used if no rerank scores supplied OR scores fail validation. **Preserves the entity-miss penalty** (corrects the original plan: fallback must keep grounding behavior, otherwise the load-bearing pattern silently disappears on bad scores). Local relevance proxy: term-overlap on `ranking_query`.
  - When fallback fires, emit `[Rerank] LLM scores missing or invalid: <reason>; falling back to local heuristic with entity-miss penalty preserved` AND add `"rerank-fallback"` to the warnings list (covers adversarial Finding 2 + F1 silent-degradation).
  - Output is **the prompt to give the host LLM**, written to `rerank_prompt.md` by U6's entry point.

  **Patterns to follow:**
  - `last30days/scripts/lib/rerank.py` lines 71-188 — the entire `_build_prompt` shape, the `UNTRUSTED_CONTENT_NOTICE`, the entity-grounding clause, and the scoring scale are taken near-verbatim. Adjust `INTENT_SCORING_HINTS` for our 5 corpus intents.

  **Test scenarios:**
  - Happy path: rerank prompt contains `<untrusted_content>` open and close tags around candidate block.
  - Happy path: rerank prompt includes the strict scoring scale lines (0-39, 40-69, 70-89, 90-100).
  - Happy path: rerank prompt includes the explicit output-schema block AND the "score only on this prompt" instruction (self-containment for context-compaction survival).
  - Happy path: when `primary_entity` is non-empty, prompt includes the grounding clause.
  - Happy path: `apply_scores` correctly sets `final_score` and applies entity-miss penalty.
  - Schema validation: well-formed scores JSON validates and round-trips; malformed shapes surface specific error strings (`"missing 'scores' key"`, `"candidate_id 'xyz' not in shortlist"`, `"relevance not an int 0-100"`).
  - End-to-end with mock-LLM (covers adversarial F7 — testability of the load-bearing pattern): a deterministic mock-LLM that scores 80 if `primary_entity` token appears in snippet else 30. Pipeline run produces a Report whose ranked order matches the deterministic rule. Verifies the rerank step works end-to-end without an external provider.
  - Edge case: candidate not in scores dict → `final_score` falls back to local fallback score WITH entity-miss penalty preserved.
  - Edge case: scores JSON validates as wrong-shaped (e.g., `{candidate_id: 0.9}` instead of nested object) → fallback fires, `"rerank-fallback"` warning added, stderr trace emitted.
  - Security: candidate snippet containing `</untrusted_content>` literal is escaped before embedding; fence remains structurally valid.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_rerank_prompt.py -q` passes.

---

### Phase 3 — Resilience (cluster + warnings only)

Goal: produce the structured Report shape. Phase-2 entity supplemental and Phase-2b thin-scope retry are **deferred** per scope-guardian review — they're adapted from social-media patterns whose marginal lift on a 4-notebook corpus isn't proven yet. Defer until production queries reveal a gap they'd actually fill.

> **Deferred to Follow-Up Work** (originally U9, U10):
> - **U9 — Phase-2 entity supplemental search.** Re-evaluate after the first 10 production queries. Bring back if Phase-1 candidate pools are consistently thin (median <8 candidates) on questions where the answer should exist.
> - **U10 — Phase-2b thin-scope retry.** Re-evaluate same trigger. For a 4-notebook corpus where scopes are topically coherent, "thin retrieval" usually means the answer isn't there — not that the query was malformed.

- [ ] **U9-DEFERRED. `entity_extract.py` + Phase-2 supplemental search in `pipeline.py`** *(deferred — see note above)*

  **Goal:** after Phase-1, extract repeated source titles + named entities from citations, run targeted follow-up `nlm` queries at lower weight.

  **Requirements:** R6.

  **Dependencies:** U5, U7.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/entity_extract.py`
  - Modify: `.claude/skills/ask-corpus/scripts/corpus_lib/pipeline.py` (add `_run_supplemental_searches`)
  - Create: `.claude/skills/ask-corpus/tests/test_entity_extract.py`

  **Approach:**
  - `extract_entities(candidates: list[Candidate]) -> list[EntityHint]` returns up to 5 hints. Sources: (a) source titles appearing in ≥2 candidates, (b) capitalized 2-word phrases from ranking queries that don't appear in the original question (likely domain-specific terms the planner discovered).
  - Pipeline integration: after Phase 1, if `len(ranked_candidates) >= 5` (heuristic — only worth running if we have signal), run one supplemental subquery per top-3 entity hint, against the same scope set, at `weight=0.3`. Register the supplemental subquery into the plan so RRF discounts properly.
  - Skip supplemental in `--depth=quick`.

  **Patterns to follow:**
  - `last30days/scripts/lib/pipeline.py:_run_supplemental_searches` (lines 576-721) for orchestration; replace handle/subreddit extraction with source-title/named-entity.

  **Test scenarios:**
  - Happy path: 5 candidates where one source title appears in 3 of them → that source title is a returned entity hint.
  - Edge case: `len(candidates) < 5` → no supplemental search runs, no extra trace lines.
  - Edge case: all candidates from different source titles, no repeated capitalized phrases → empty hints, no supplemental search.
  - Integration: supplemental results land in the bundle under a new label `"supplemental-related"` with `weight=0.3` registered into `plan.subqueries`.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_entity_extract.py -q` passes.

- [ ] **U10-DEFERRED. `query.py` + Phase-2b thin-scope retry in `pipeline.py`** *(deferred — see note above)*

  **Goal:** scopes that returned <3 useful citations get retried with a simplified core-subject query.

  **Requirements:** R7.

  **Dependencies:** U5, U7.

  **Files:**
  - Create: `.claude/skills/ask-corpus/scripts/corpus_lib/query.py`
  - Modify: `.claude/skills/ask-corpus/scripts/corpus_lib/pipeline.py` (add `_retry_thin_scopes`)

  **Approach:**
  - `extract_core_subject(question, max_words=3) -> str` — drop stopwords + intent-modifier phrases (matches last30days's `_INTENT_MODIFIER_RE` shape, adjusted for our intents), keep the longest remaining named-entity-shaped substring.
  - Pipeline integration: scopes with `<3` items in their merged stream get retried at `weight=0.3` under label `"retry"`.
  - Skip retry in `--depth=quick`.

  **Patterns to follow:**
  - `last30days/scripts/lib/pipeline.py:_retry_thin_sources` (lines 724-823).

  **Test scenarios:**
  - Happy path: scope `methodology.metabase` returned 1 item → retry runs with `extract_core_subject(question)` → augments scope's stream.
  - Edge case: scope already errored → not retried.
  - Edge case: `extract_core_subject` returns empty string → retry skipped, trace line records skip reason.

  **Verification:**
  - Smoke run with a question deliberately phrased to be too narrow (e.g., "Metabase Cloud SQL automated backup retention default for the dbt-metabase plugin in 2026") shows retry running and adding items.

- [x] **U11. Report finalization + `_warnings` (subquery grouping, no separate cluster module)** *(landed 2026-04-26; 10 finalize tests; group_by_subquery + warnings_for moved into pipeline.py; CLI now consumes them; multi-label cluster assignment uses best native_rank; 126/126 cumulative pass)*

  **Goal:** the final shape — candidates grouped by their originating subquery, structured warnings, the `Report` object the host LLM consumes for synthesis.

  **Requirements:** R1, R9.

  **Dependencies:** U7, U8.

  **Files:**
  - Modify: `.claude/skills/ask-corpus/scripts/corpus_lib/pipeline.py` (add `_finalize_report`, `_group_by_subquery`, `_warnings`)
  - Create: `.claude/skills/ask-corpus/tests/test_finalize.py`

  **Approach:**
  - **No separate `cluster.py` module at v2.0.** Per scope-guardian review: "cluster by subquery.label" is grouping, not semantic clustering — fold inline into pipeline finalization. Real semantic clustering (grouping by topic similarity across subqueries) becomes a future enhancement when production queries show the need.
  - `_group_by_subquery(ranked: list[Candidate], plan: QueryPlan) -> list[Cluster]`: each surviving subquery's matched candidates become one `Cluster` with `theme = subquery.ranking_query`. Candidates that match multiple subqueries land in their highest-scoring subquery only.
  - `_warnings(report) -> list[str]`:
    - `"thin-evidence"` if `len(ranked_candidates) < 5`
    - `"scope-concentration"` if top-5 candidates are all from one scope
    - `"scope-errors"` if any scope failed
    - `"no-usable-items"` if `len(ranked) == 0`
    - `"plan-fallback"` if `plan_source == "deterministic"` (host LLM didn't supply a plan)
  - Pipeline finalization writes `Report` to `report.json`.

  **Patterns to follow:**
  - `last30days/scripts/lib/pipeline.py:_warnings` (lines 536-557) for warnings list.

  **Test scenarios:**
  - Happy path: 3 subqueries with matched candidates → 3 clusters.
  - Edge case: candidates that match multiple subquery labels are placed in their highest-scoring cluster only (no duplication).
  - Warnings: thin-evidence + scope-concentration + plan-fallback all fire when conditions hold; warnings empty when 12 candidates spread across 3 scopes from a host-supplied plan.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests/test_cluster.py -q` passes.
  - `report.json` shape validates against `schema.Report` reflective check.

---

### Phase 4 — Skill contract (host LLM facing)

Goal: the SKILL.md the host LLM reads, which is what makes the engine actually usable.

- [x] **U12. SKILL.md v2 — voice contract + LAWs + handshake protocol** *(landed 2026-04-26; v1 backed up to SKILL-v1.md; new SKILL.md covers STEP 0 / CONTRACT / OUTPUT CONTRACT badge+3 LAWs / PLAN GENERATION RULES / HANDSHAKE PROTOCOL / SYNTHESIS TEMPLATE / LAW ANCHORS / WHEN TO USE / COST NOTE; mart-naming anchor preserved as LAW 3)*

  **Goal:** rewrite `.claude/skills/ask-corpus/SKILL.md` as a contract that:
    - Tells the host LLM how to generate a plan JSON (rules adapted from last30days's planner prompt rules)
    - Documents the two-phase handshake (`--phase=retrieve` → score → `--phase=finalize`)
    - Carries forward LAWs 1–5 from last30days adapted to corpus output
    - Specifies the synthesis voice (badge, body shape, KEY PATTERNS list, engine footer, invitation)
    - Includes the 2026-04-19 mart-naming incident as the first inline LAW anchor

  **Requirements:** R1, R10.

  **Dependencies:** U6, U8 (so the handshake is real and the rerank prompt is concrete).

  **Files:**
  - Modify: `.claude/skills/ask-corpus/SKILL.md` (full rewrite)
  - Backup: rename current `SKILL.md` → `SKILL-v1.md` for reference; remove after U16 ships.

  **Approach:**
  - **Section order:**
    1. Frontmatter (name, description, version: "2.0.0")
    2. STEP 0 — read the voice contract before anything else
    3. CONTRACT — what `ask-corpus` is and isn't
    4. OUTPUT CONTRACT (BADGE + LAWS) — three LAWs only at v2.0
    5. PLAN GENERATION RULES — adapted from `last30days/planner.py:_build_prompt`. Critical rules: never include temporal hedges, strip intent-modifier phrases, two query forms per subquery, prefer multi-subquery for design/howto intents.
    6. HANDSHAKE PROTOCOL — exact CLI invocation for retrieve and finalize phases.
    7. SYNTHESIS TEMPLATE — body shape per intent, KEY PATTERNS list, engine footer, invitation. Formatting preferences (no em-dashes, no `##` body headers, no invented title) live here as template guidance, not as LAWs.
    8. LAW ANCHORS — incidents tied to LAWs (mart-naming = LAW 3).
    9. WHEN TO USE / SKIP.
    10. COST NOTE — `nlm` calls are free; rerank is host LLM (no extra quota).
  - **Three LAWs at launch** (each tied to a real anchor):
    - LAW 1 — **No `Sources:` block at the end.** Engine emits citations inline in synthesis-ready form; the WebSearch tool's "MUST include Sources:" reminder is overridden inside the skill. (Architecture-driven; carried from last30days.)
    - LAW 2 — **Engine footer pass-through.** The Python engine emits a structured citations + warnings footer; the host LLM copies it verbatim, never rewrites. (Mechanical; preserves trace fidelity.)
    - LAW 3 — **Always double-check the corpus before locking advice into a rule.** Anchor: 2026-04-19 mart-naming incident — first-principles reasoning was directionally correct but missed three actionable specifics the corpus had ready. The whole engine exists to make this surfacing automatic, but the LAW reinforces the discipline at synthesis time.
  - **No LAW 4/5/6 at v2.0.** Em-dashes, `##` headers, invented titles are formatting preferences in the synthesis template — not failure-mode anchors. Promote to LAW status only when an incident in this skill produces the failure mode.

  **Patterns to follow:**
  - `last30days/SKILL.md` lines 82-148 (LAWs section), structurally; adapt LAWs to corpus domain.
  - Voice contract precedence (LAWs override personal-memory preferences inside the skill) — same clause as last30days SKILL.md:117-119.

  **Test scenarios:**
  - Test expectation: none — manual review. The verification is "does Claude produce correct output when invoked?" which is qualitative. Run 3 spot-check questions after U15 (one per intent class) and confirm output shape.

  **Verification:**
  - Manual review of SKILL.md against checklist: badge anchor present, 3 LAWs present, handshake protocol exact, synthesis template per intent, mart-naming incident referenced as LAW 3.

- [ ] **U13. Test suite hardening + fixtures from real `nlm` runs**

  **Goal:** capture real `nlm` responses as JSON fixtures; expand smoke test to cover 3+ representative questions; lock in the test discipline.

  **Requirements:** R13.

  **Dependencies:** U3, U5, U7, U8, U11.

  **Files:**
  - Create: `.claude/skills/ask-corpus/tests/fixtures/nlm_data_ops_star_schema.json`
  - Create: `.claude/skills/ask-corpus/tests/fixtures/nlm_metabase_backup.json`
  - Create: `.claude/skills/ask-corpus/tests/fixtures/nlm_engagement_speed_to_lead.json`
  - Modify: `.claude/skills/ask-corpus/tests/test_pipeline_smoke.py` (3 scenarios)

  **Approach:**
  - One-time capture script: invoke `nlm notebook query <id> "<q>"` against each of the four notebooks for one representative question each; commit raw JSON to `fixtures/`.
  - Smoke test runs the full retrieve+finalize cycle against fixtures using a stub for `subprocess.run`.
  - Add property-style check: `report.json` validates against `schema.Report`.

  **Patterns to follow:**
  - `last30days/tests/test_pipeline_*` shapes.

  **Test scenarios:**
  - Happy path: smoke test with `methodology` scope (3 notebooks) returns a Report with ≥3 ranked candidates and clusters keyed by subquery label.
  - Happy path: smoke test with `engagement` scope returns a Report with `engagement` candidates only.
  - Happy path: smoke test simulating `--depth=quick` skips Phase-2 supplemental and Phase-2b retry.

  **Verification:**
  - `pytest .claude/skills/ask-corpus/tests -q` passes; coverage report shows >70% line coverage on `corpus_lib/*.py`.

---

### Phase 5 — Documentation cutover

Goal: the rest of the repo knows v2 exists; v1 stops being the path.

- [x] **U14. Update `using-the-notebook.md` rule to reference v2 + remove v1 inline snippet** *(landed 2026-04-26; rule now points at v2 engine, drops the inline bash+python snippet, mart-naming anchor cross-references LAW 3 in SKILL.md, cost note updated to clarify nlm/host-LLM call mix)*

  **Goal:** the canonical routing rule reflects v2's contract.

  **Requirements:** R10.

  **Dependencies:** U12.

  **Files:**
  - Modify: `.claude/rules/using-the-notebook.md`

  **Approach:**
  - Replace the existing "Query the notebook first" section with: "Invoke the `ask-corpus` skill (v2). It runs a planner→fan-out→fuse→rerank→cluster pipeline and returns a structured Report which the synthesis voice contract converts into a cited answer."
  - Replace the inline `notebook_query` / `cross_notebook_query` examples with: "The skill handles retrieval; do not call MCP tools directly when the routing rule applies."
  - Keep the "Always double-check" section (LAW 6 anchor); update the mart-naming example to note it's now codified as LAW 6 in the skill.
  - Keep the corpora declaration table (resolved from `corpus.yaml` — unchanged).
  - Keep the PostToolUse auto-sync section (unchanged).
  - Bump rule's implicit version (no field, but update via worklog).

  **Test scenarios:**
  - Test expectation: none — documentation change. Verification is "does the rule still describe correct behavior" — manual review.

  **Verification:**
  - Read-through review: rule references v2, mart-naming anchor preserved, no stale inline MCP-call snippets.
  - PostToolUse hook fires on save and re-syncs the rule into Data Ops notebook (sync log: `/tmp/dataops-sync-rule.log`).

- [ ] **U15. Worklog entry + project-state index refresh + corpus.yaml comment touch-up + smoke run**

  **Goal:** the engagement's audit log reflects this shipped; the project-state index is fresh; one final end-to-end smoke run.

  **Requirements:** R10.

  **Dependencies:** U14 (everything else).

  **Files:**
  - Modify: `WORKLOG.md` (new dated entry)
  - Modify: `.claude/state/project-state.md` (regenerate per `.claude/rules/worklog.md`)
  - Modify: `.claude/corpus.yaml` (add a top-of-file comment noting v2 reads optional `weight` and `size_hint` keys; no schema change)
  - Optionally: stub `docs/solutions/2026-04-26-corpus-research-engine.md` for any incident captured during build.

  **Approach:**
  - Worklog entry per `.claude/rules/worklog.md` shape: What happened, Decisions, Open threads.
  - Project-state index: bump "Where we are" if Phase A still active (sprint hasn't ended); add v2 ask-corpus to "Where to look".
  - Final smoke: invoke the skill from a fresh Claude Code session against three representative questions (one design, one ops, one history) and confirm the Report → synthesis path works without manual intervention.

  **Test scenarios:**
  - Test expectation: none — bookkeeping.

  **Verification:**
  - Worklog entry committed.
  - `bash .claude/scripts/sessionstart-inject-state.sh | jq -r '.hookSpecificOutput.additionalContext' | head -n 20` shows v2 reflected.
  - Three smoke questions return Report-grounded synthesis with engine footer and at least one warning resolved correctly.

---

## System-Wide Impact

- **Interaction graph:** the `using-the-notebook.md` rule (loaded path-scoped on every file edit) currently expects MCP `notebook_query` invocations. After U14 it points at the v2 skill. The PostToolUse rule-sync hook is unaffected.
- **Error propagation:** retrieval errors per scope are captured as `errors_by_scope` in the bundle; if all scopes fail, the engine exits non-zero (3) with `[Pipeline] all scopes errored: ...` trace and the host LLM is expected to surface that to the user as "the corpus is unreachable, falling back to first principles."
- **State lifecycle risks:** the engine writes `shortlist.json`, `rerank_prompt.md`, and `report.json` to `$TMPDIR/ask-corpus-<rand>/`. These are session-scoped; do not persist. Add to `.gitignore` defensively if anyone runs the engine inside the repo with `--out-dir=.`.
- **API surface parity:** the existing MCP path (`mcp__notebooklm-mcp__notebook_query` / `cross_notebook_query`) remains available for ad-hoc queries. v2 owns the orchestrated path; raw MCP stays usable for one-off "just look this up" calls outside the skill.
- **Integration coverage:** mocks in tests cover the `subprocess.run` boundary; the live smoke run in U15 is the integration coverage that proves end-to-end against real `nlm`.
- **Unchanged invariants:** `corpus.yaml` schema (additive optional keys only); the four notebook IDs; the PostToolUse auto-sync hook; the cost note ("`notebook_query` is free"); the `using-the-notebook.md` "always double-check" discipline.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| **Two-phase rerank handshake is novel architecture, not a port** — host LLM may not reliably execute the second `--phase=finalize` call, may produce malformed JSON, may go off-protocol mid-handshake | Mandatory `validate_rerank_scores` in U8; `_local_fallback` preserves entity-miss penalty (so silent degradation is bounded); `"rerank-fallback"` warning makes degraded runs visible; rerank prompt is fully self-contained so context compaction between phases doesn't blind the scorer; SKILL.md U12 reinforces the protocol |
| **No `source_title` in `nlm notebook query --json`** — per-source-id cap relies on `source_id` (verified stable); titles are fetched lazily after rerank | Per-source-id cap in U7 keys on `source_id`. Lazy `list_sources` call in U3 is cached per-process. If `nlm source list --json` shape changes, that's a single test fixture to refresh |
| `nlm` CLI surface drift between versions could break the subprocess wrappers | Pin to current behavior with fixtures captured in U13; treat any post-update test failure as a one-line constant change |
| Authority-asymmetry diversity guard threshold (0.30) is empirically tuned | Make it a constant in `fusion.py`; document in the comment that it's empirical; revisit after the first 5 production queries by skim-reading whether under-represented scopes get sensible reservations |
| Rerank prompt prompt-injection: a candidate snippet contains `</untrusted_content>` literal | Sanitize candidate content in `_build_prompt` — strip or escape the literal tag before embedding |
| Host LLM ignores the `--plan` handshake and just calls the engine bare | Stderr nudge from `planner._fallback_plan` (modeled on last30days's LAW 7 nudge); SKILL.md U12 reinforces; warnings list emits `"plan-fallback"` so synthesis can flag it |
| Test fixtures go stale as notebooks evolve (rule sync upserts new content) | Fixtures are committed to git; refresh on a separate cadence as part of corpus maintenance, not gated to every PR |
| Phase 2 entity-extract heuristic is too crude for low-signal corpora | Skip threshold (`len(candidates) >= 5`) gates the supplemental search; if it never fires for engagement-scope queries, that's a tuning task not a correctness bug |
| The engine's TMPDIR-scoped JSON files clutter `/tmp` over long sessions | Use `tempfile.TemporaryDirectory` in U6's entrypoint when `--out-dir` not supplied; fall back to user-supplied path otherwise |
| Solo-operator scope creep: "while we're at it, let's add provider plumbing" | Scope Boundaries explicitly defers `providers.py`-equivalent; cite it if temptation surfaces mid-build |

---

## Documentation / Operational Notes

- **Sync log:** stderr traces are not persisted; if a debug session needs them, redirect: `python3 corpus_research.py --phase=retrieve --question "..." 2>/tmp/corpus-trace.log`.
- **Smoke ritual:** after any change to `planner.py` rules or `rerank.py` prompt, run the three smoke questions in U15 and review output for regression. Add new failure-mode anchors to SKILL.md LAWs as they surface (per the worklog rule's "lessons learned" pattern).
- **Compatibility window:** v1 SKILL.md remains as `SKILL-v1.md` until U16's smoke run validates v2; can be removed in a follow-up cleanup PR.
- **Future external publication:** if/when this skill ever ships outside the engagement, the STEP 0 canonical-path self-check from `last30days/SKILL.md:62-78` becomes relevant; not needed while it stays repo-local.

---

## Phased Delivery

### Phase 1 (U1-U3) — Skeleton (target: 1 session)
Scaffolding + dataclasses + `corpus.yaml` resolver + `nlm` subprocess wrappers. Prove the plumbing end-to-end with a single hardcoded subquery against one notebook before adding any sophistication.

### Phase 2 (U4-U8) — Pipeline core (target: 1-2 sessions)
Planner + entry point + fan-out + fusion + rerank prompt. After Phase 2 lands, the engine runs the full handshake and returns ranked candidates with diversity guards. Functional parity with last30days's *retrieval* core, scoped to corpus.

### Phase 3 (U11) — Report finalization (target: 0.5 session)
Inline `_finalize_report` + `_warnings` in `pipeline.py`. After this, the engine emits the structured Report the synthesis voice contract consumes. *(U9 + U10 deferred to follow-up — not in this build's critical path.)*

### Phase 4 (U12-U13) — Skill contract (target: 1 session)
SKILL.md voice contract + test suite hardening. After Phase 4, invoking the skill from a Claude Code session produces correct, synthesized output.

### Phase 5 (U14-U15) — Documentation cutover (target: 0.5 session)
Update routing rule, worklog, project-state index, smoke run. Engine is canonical from this point.

**Total target:** 3-4 build sessions (revised down from 4-5 after deferring U9/U10 and folding cluster.py into U11). No HARD GATEs (no parity contracts to lock); the engine is internally consistent and reversible at each phase. **Revised unit count: 11 active + 2 deferred (U9, U10) = 13 total (was 15).**

---

## Alternative Approaches Considered

- **Markdown-only skill or hybrid (markdown + Python helpers).** Both rejected per the architectural decision in planning. Either route forfeits the test surface, structured traces, and reproducibility that make last30days's rigor real. The hybrid in particular ends up reimplementing 80% of the engine in Python helpers while leaving the orchestration LLM-driven — harder to reason about than pure-Python with no offsetting benefit.
- **Build a `providers.py`-equivalent so the engine can self-rerank without host LLM.** Deferred. Useful for headless cron, irrelevant for interactive use, and adds an external API key dependency the engagement doesn't currently have. Reconsider if/when scheduled corpus runs become valuable.
- **Reuse last30days as a forked engine.** Rejected: too much surface area is socialmedia-specific (yt-dlp, ScrapeCreators, bird CLI, Gamma API). The architectural patterns are what's portable; the implementation is not. Cleaner to build a focused engine that *imitates* than to fork and gut.
- **Skip the rerank step (fusion-only output).** Rejected: the rerank step is where the primary-entity grounding penalty fires. Fusion alone can't catch the "adjacent-but-irrelevant citation" failure mode (the Hermes Agent / mart-naming class of incidents). This is the load-bearing pattern for sharpness.
- **Single-pass retrieval (skip fan-out).** Rejected: the whole point of v2 is that single-pass retrieval misses authoritative specifics. Eliminating fan-out would defeat the architecture.

---

## Sources & References

- **Origin conversation:** 2026-04-26 — analysis of `last30days-skill` v3.1.0 architecture (`pipeline.py`, `planner.py`, `fusion.py`, `rerank.py`, `SKILL.md`); decision to mirror it for corpus retrieval; AskUserQuestion choice on engine shape (Python — last30days parity).
- **Upstream architecture:** `~/.claude/plugins/cache/last30days-skill/last30days/3.1.0/scripts/lib/`
- **Repo files this plan modifies or creates:** `.claude/skills/ask-corpus/`, `.claude/rules/using-the-notebook.md`, `.claude/corpus.yaml` (comment-only), `WORKLOG.md`, `.claude/state/project-state.md`.
- **Repo plan precedent for voice / structure:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`.
- **Corpus rule (preserved invariants):** `.claude/rules/using-the-notebook.md`.
- **External:** Cormack et al. 2009, "Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods" — RRF_K=60 origin.

---

## Revision Log

**2026-04-26 — initial plan + same-day deepening pass.** Doc review (4 reviewers, parallel) surfaced findings that materially changed the design before any code landed. Captured here so future readers see why the plan reads the way it does.

### Architecture corrections (from feasibility + adversarial reviews)
- **Dropped `nlm cross query` path entirely.** Verified at plan time: `nlm cross query --json` is not supported. Fan-out via parallel `nlm notebook query --json` calls instead.
- **Replaced "per-source-title cap" with "per-source-id cap".** Verified at plan time: `nlm notebook query --json` returns `{source_id, citation_number, cited_text}` only — no `source_title` field. Titles are fetched lazily via `nlm source list --json` *only for post-rerank candidates*, with per-process caching. Hot retrieval path stays single-API-call per scope.
- **Capped `ThreadPoolExecutor` workers at 3 (was 8).** Default-scope query already issues 9 calls; higher concurrency risks NotebookLM throttling without measured headroom.
- **Reframed the two-phase rerank handshake as novel architecture, not parity.** last30days runs rerank engine-internal via `providers.py`. Our host-LLM-as-reranker JSON handshake is new design with no upstream battle-testing — required mitigations (schema validation, self-contained prompt, mock-LLM tests) are now explicit in U8.
- **Quality-aware diversity guard.** Pure count-based reservation can inject mediocre citations and worsen answers. Added quality-parity floor: a scope reserves slots only when its top item's relevance is ≥0.6 of the dominant scope's top item. Below that, RRF merit alone.
- **Mandatory `validate_rerank_scores` + `"rerank-fallback"` warning.** Malformed scores from the host LLM no longer silently degrade — warnings surface degraded runs at synthesis time; fallback preserves entity-miss penalty.
- **Self-contained rerank prompt** (covers context-compaction between phases). Prompt embeds its own output schema and "score only on this prompt" instruction.

### Scope corrections (from scope-guardian review)
- **Dropped standalone `pyproject.toml`.** Root `pyproject.toml` already configures ruff (py311 target, full lint rules); nesting a second one creates competing configs. U1 inherits root config.
- **Deferred U9 (Phase-2 entity supplemental) and U10 (Phase-2b thin-scope retry).** Adapted from social-media patterns whose marginal lift on a 4-notebook corpus isn't proven. Re-evaluate after the first 10 production queries reveal a real gap.
- **Folded U11 cluster.py into pipeline finalization.** "Cluster by subquery label" is grouping, not clustering — no separate module needed. Real semantic clustering becomes a future enhancement when production queries show the need.
- **Scoped LAWs from 6 to 3 at v2.0 launch.** Earned, not cargo-culted: (1) no `Sources:` block, (2) engine footer pass-through, (3) always-double-check (mart-naming anchor). Em-dashes / `##` headers / invented titles are formatting preferences in the synthesis template, not LAWs. Promote to LAW status only when an incident in this skill produces the failure mode.
- **Trimmed Alternative Approaches.** Markdown-only and hybrid had identical rejection rationale — collapsed to one entry.

### Failure-mode reframe (from adversarial review Finding 3)
- **Mart-naming incident anchors LAW 3 (always-double-check), NOT the rerank step.** The mart-naming root cause was *skipping the corpus query entirely* — a policy bypass, not an adjacent-citation problem. The engine's rerank step addresses a different (Hermes-class) failure mode that has not occurred in this engagement yet. Both are real; conflating them muddied the original plan's justifications.

### Coherence corrections (from coherence review)
- LAW count contradiction (5 vs 6) resolved by scoping to 3 (above).
- R5 phase naming corrected: rerank prompt emitted at `--phase=retrieve`; consumed at `--phase=finalize`. No `--phase=rerank` exists.

### Estimate revision
- Was: 4-5 sessions across 5 phases / 15 units. Now: 3-4 sessions across 5 phases / 11 active + 2 deferred = 13 total units. Feasibility review flagged 4-5 as optimistic for novel-design work; cutting U9/U10 plus folding U11 absorbs that risk by reducing scope.
