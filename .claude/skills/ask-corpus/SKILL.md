---
name: ask-corpus
description: Query the project's NotebookLM corpora for cited answers about data engineering concepts, dbt conventions, modeling patterns (star schema, SCDs, medallion), 3-layer pipelines, CI/CD workflows, modern data stack theory, Metabase operations/integration, and engagement-specific decisions. Use when the user asks "what does the style guide say about...", "how should I structure...", "why do we...", or when Claude is about to scaffold a dbt model, a `.claude/rules/*.md` file, a macro, or a workflow and wants grounded guidance before writing. Also triggers on "ask the corpus", "check the notebook", "what do the sources say", "ground this in the notebook".
version: "2.0.0"
---

# Ask Corpus — v2 (planner / fan-out / fuse / rerank)

## STEP 0 — Read the voice contract before anything else

Before generating a plan, scoring candidates, or synthesizing a final answer, **read this entire SKILL.md once** in this conversation. The engine emits structured output that is only useful if you obey:

1. The PLAN GENERATION RULES (so the planner has 1–5 well-formed subqueries).
2. The HANDSHAKE PROTOCOL (so the two phases compose).
3. The OUTPUT CONTRACT — especially the three LAWs.
4. The SYNTHESIS TEMPLATE (so the user-facing answer carries the engine's signal cleanly).

Skipping any of these silently degrades retrieval quality. The engine cannot enforce them on its own — that's why this contract exists.

---

## CONTRACT — what `ask-corpus` is and isn't

**`ask-corpus` is a planner-driven, fan-out / fuse / rerank engine** that turns a user question into a structured `Report` of ranked, cited evidence from the project's NotebookLM corpora. The Report is the synthesis input — you (the host LLM) convert it into the user-facing answer using the SYNTHESIS TEMPLATE below.

**`ask-corpus` is not** a chat with NotebookLM, a single `notebook_query` wrapper, or a free-text-in / free-text-out tool. The MCP `mcp__notebooklm-mcp__notebook_query` and `cross_notebook_query` tools remain available to you for ad-hoc lookups, but they are not the path this skill drives.

The engine lives at `.claude/skills/ask-corpus/scripts/corpus_research.py`. It runs against four notebooks declared in `.claude/corpus.yaml` (no IDs hardcoded here).

---

## SCOPE ROUTING — pick the right corpus(es)

`.claude/corpus.yaml` declares four notebooks. Pick the `--scope` (and per-subquery `scopes`) that matches the question. When in doubt, leave `--scope` unset — the default is `methodology`, which cross-queries all craft notebooks.

| scope value                   | covers                                                                                                |
| ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `methodology.data_ops`        | dbt conventions, modeling, warehouse design, CI/CD, MDS starter guides                                |
| `methodology.metabase`        | Metabase **ops / integration / licensing**: install, Cloud SQL backup, upgrade, dbt-metabase, BQ cost, AGPL, Metabot |
| `methodology.metabase_learn`  | Metabase **how-to / authoring / SQL**: how to build a dashboard or question, which chart to pick, drill-through, SQL tutorials, BI-tool transition guides |
| `methodology` *(default)*     | Cross-query all craft notebooks when you aren't sure which has the answer                             |
| `engagement`                  | D-DEE history: scope decisions, oracle numbers, prior client conversations                            |

Rules of thumb:

- Writing a portable `.claude/rules/*.md`? Use `methodology` (default) so a Metabase rule stays cross-checked against Data Ops conventions and vice versa.
- Writing an *operational* Metabase file (install doc, connector config, dbt-metabase YAML, Cloud SQL backup plan)? Use `methodology.metabase`.
- Writing an *authoring* Metabase decision (dashboard tile, visualization choice, filter wiring, SQL question pattern)? Use `methodology.metabase_learn`.
- Unsure which Metabase notebook applies? Use `methodology` — it cross-queries both plus Data Ops for free.
- Writing a D-DEE-only mart, oracle reconciliation, or client-facing deliverable? Use `engagement` to ground it in what was already decided.
- Cost is zero; over-querying is fine.

When you add a new methodology notebook, update `.claude/corpus.yaml` first, then add a row to this table.

---

## OUTPUT CONTRACT — Badge + LAWs

Every synthesized answer **must** start with this badge line on its own line, before any prose:

```
🟦 Sources: ask-corpus v2 ({intent}, {n_subqueries} subqueries × {n_scopes} scopes; {n_warnings} warnings)
```

Fill the variables from the Report's `intent`, `plan.subqueries`, `trace_summary`, and `warnings`. The badge tells the user the answer is corpus-grounded and signals retrieval breadth. Never omit it.

### The three LAWs (v2.0)

These override personal-memory preferences inside this skill. They are earned anchors — each tied to a real failure mode — not cargo-culted ceremony.

**LAW 1 — No `Sources:` block at the end.**
The engine emits citations inline in synthesis-ready form. Any "Sources:" reminder you may have absorbed from WebSearch context (or other tools) is overridden inside this skill. Inline citation only.

**LAW 2 — Engine footer pass-through.**
The Report contains a citation footer + warnings list. Copy them verbatim into the synthesized answer. Never rewrite, paraphrase, or "clean up" the footer — it is the user's audit trail.

**LAW 3 — Always double-check the corpus before locking advice into a rule.**
Anchor incident: 2026-04-19 mart-naming question. First-principles reasoning produced a directionally-correct answer ("multiple dashboards, one shared mart layer") but missed three actionable specifics the corpus had ready: separate by *schema*, drop `fct_/dim_` prefixes in marts, fewer wider marts over many narrow ones. Without the double-check, the rule would have shipped incomplete in a way that costs the client clarity. This whole engine exists to make that surfacing automatic — but the LAW reinforces the discipline at synthesis time. **If the user asks "should we…?" or "what's the convention for…?", and you have not run `ask-corpus` against the relevant scope this turn, run it now.**

---

## PLAN GENERATION RULES

When the engine fires `--phase=retrieve` without `--plan`, it emits a stderr nudge telling you (the host LLM) to generate a plan. **You are the planner.** You do not need an API key — you ARE the LLM.

A plan is a JSON object with this shape:

```json
{
  "intent": "convention | ops | howto | history | design",
  "scope_weights": { "methodology.data_ops": 1.0, "methodology.metabase": 1.0, ... },
  "subqueries": [
    {
      "label": "primary",
      "search_query": "keyword-style query for nlm retrieval",
      "ranking_query": "natural-language rewrite for the reranker",
      "scopes": ["methodology.data_ops", "methodology.metabase"],
      "weight": 1.0
    }
  ],
  "notes": ["optional short notes"]
}
```

### Rules

- **Pick the right intent** for the question. Five choices, corpus-domain-specific:
  - `convention`: naming, style, structural rules ("what do we call X?", "should we prefix Y?")
  - `ops`: operational tasks (deploy, backup, upgrade, service accounts, BigQuery cost)
  - `howto`: step-by-step ("how do I build a dashboard tile?", "tutorial for SCDs")
  - `history`: this engagement's past decisions ("what did we decide about Speed-to-Lead?", "back when we picked Cloud Run")
  - `design`: architecture / trade-offs ("star schema vs medallion", "which mart layer should this live in?")
- **Emit 1–5 subqueries.** Use 1 for crisp factual asks; 3–5 for design/howto where multiple angles help fusion.
- **Two query forms per subquery.** `search_query` is keyword-style for `nlm` retrieval (think titles and bullet points). `ranking_query` is the natural-language form for the reranker. Conflating them is the most common failure mode.
- **Scopes drawn from the available list.** Today: `methodology.data_ops`, `methodology.metabase`, `methodology.metabase_learn`, `engagement`. Anything else is filtered out.
- **Strip hedging from `search_query`.** Don't include "what does the corpus say about", "how should we", "please tell me". Bare keyword strings retrieve more than echoed full questions.
- **Preserve proper nouns.** "Speed-to-Lead", "GHL", "BigQuery", "Cloud Run" — keep them spelled exactly.
- **Don't over-cap subqueries with `weight`.** Stay between 0.5 and 1.5; the engine normalizes.
- **Default `scope_weights` to 1.0 each** unless one notebook is genuinely the authority (e.g., `engagement` for history; `methodology.metabase_learn` for dashboard-authoring how-to).

If you skip the `--plan` and let the engine fall back to a deterministic single-subquery plan, the engine emits a `plan-fallback` warning and the synthesis must surface that under "Caveats" — that's the cost of cutting the corner.

---

## HANDSHAKE PROTOCOL

Two phases. Each prints one JSON line to stdout that you parse to find the next file to read.

### Phase 1 — retrieve

```bash
.claude/skills/ask-corpus/.venv/bin/python \
  .claude/skills/ask-corpus/scripts/corpus_research.py \
  --phase=retrieve \
  --question "<user question, verbatim>" \
  --scope "<methodology | methodology.<key> | engagement>" \
  --plan /tmp/plan.json
```

Write the plan JSON to `/tmp/plan.json` (or any path) before invocation. The engine prints:

```json
{"shortlist": "/var/folders/.../shortlist.json", "rerank_prompt": "/var/folders/.../rerank_prompt.md"}
```

Read the `rerank_prompt` markdown file in your next turn. It is fully self-contained — score the candidates per the embedded scale, output a JSON object with this shape:

```json
{
  "scores": [
    {"candidate_id": "<id from shortlist>", "relevance": 0-100, "reason": "short reason"}
  ]
}
```

Write that to a file (e.g., `/tmp/scores.json`).

### Phase 2 — finalize

```bash
.claude/skills/ask-corpus/.venv/bin/python \
  .claude/skills/ask-corpus/scripts/corpus_research.py \
  --phase=finalize \
  --shortlist /var/folders/.../shortlist.json \
  --rerank-scores /tmp/scores.json
```

The engine prints:

```json
{"report": "/var/folders/.../report.json"}
```

Read that file. It carries:

- `question`, `intent`, `primary_entity`
- `plan` (the sanitized plan you emitted)
- `ranked_candidates` (each with `final_score`, `rerank_reason`, `snippet`, `scope`, `source_id`)
- `clusters` (one per surviving subquery; each with `theme` and ordered candidates)
- `warnings` (structured list — see "Synthesizing warnings" below)
- `trace_summary`

Synthesize per the SYNTHESIS TEMPLATE.

### When you skip a step

- **No `--plan`:** the engine emits `plan-fallback`, runs a single-subquery deterministic plan, retrieval is shallower. Surface this in synthesis.
- **No `--rerank-scores` on finalize:** the engine emits `rerank-fallback`, falls back to local-relevance scoring with the entity-miss penalty preserved. Surface this in synthesis.

---

## SYNTHESIS TEMPLATE

After reading `report.json`, produce the user-facing answer in this shape:

```
🟦 Sources: ask-corpus v2 ({intent}, {n_subqueries} subqueries × {n_scopes} scopes; {n_warnings} warnings)

What the sources say:

**{Bold lead-in for the first claim}** — supporting prose with inline citations like (source: "<source_title>", <scope>). Group claims by subquery cluster when more than one cluster surfaced. Tight paragraphs; no `##` body headers.

**{Bold lead-in for the second claim}** — keep paragraphs short. If the user asked a yes/no question, answer it explicitly in the first paragraph.

KEY PATTERNS from the corpus:

1. {First pattern, one sentence, with citation}.
2. {Second pattern, one sentence, with citation}.
3. ...

{Engine footer — copy verbatim from the Report's warnings + top citations. LAW 2.}

{Optional invitation: "Want me to drill into <X> or <Y>?"}
```

### Body conventions (template guidance, not LAWs)

- No `##` body headers.
- No em-dashes used as parenthetical punctuation; use sentence breaks instead.
- No invented title above the badge — the badge is the title.
- Inline citations only; never an end-of-answer "Sources:" block (LAW 1).

### Worked example — end-to-end handshake

Concrete walkthrough so the abstract protocol above is unambiguous.

User question: *"How should I prioritize which marts to build first when I have a mix of Speed-to-Lead, churn, and revenue-attribution candidates?"*

**Step 1 — Author the plan.** Write to `/tmp/plan.json`:

```json
{
  "intent": "design",
  "scope_weights": { "methodology.data_ops": 1.0, "engagement": 0.8 },
  "subqueries": [
    {
      "label": "prioritization-frameworks",
      "search_query": "mart prioritization value feasibility ranking",
      "ranking_query": "How do experienced data teams pick which marts to build first?",
      "scopes": ["methodology.data_ops"],
      "weight": 1.0
    },
    {
      "label": "first-mart-pattern",
      "search_query": "first mart star schema starting point grain",
      "ranking_query": "What makes a defensible first mart in a new warehouse?",
      "scopes": ["methodology.data_ops"],
      "weight": 1.0
    },
    {
      "label": "engagement-priors",
      "search_query": "Speed-to-Lead mart decisions priorities D-DEE",
      "ranking_query": "What has D-DEE already decided about mart priorities?",
      "scopes": ["engagement"],
      "weight": 0.8
    }
  ],
  "notes": ["Mart roadmap deliverable; LAW 3 applies"]
}
```

**Step 2 — Phase 1 retrieve.**

```bash
.claude/skills/ask-corpus/.venv/bin/python \
  .claude/skills/ask-corpus/scripts/corpus_research.py \
  --phase=retrieve \
  --question "How should I prioritize which marts to build first..." \
  --plan /tmp/plan.json
```

stdout (one JSON line):

```json
{"shortlist": "/tmp/.../shortlist.json", "rerank_prompt": "/tmp/.../rerank_prompt.md"}
```

**Step 3 — Score the rerank prompt.** Read `rerank_prompt.md` (self-contained — embedded scale, embedded snippets). Output scores JSON:

```json
{
  "scores": [
    {"candidate_id": "abc-1", "relevance": 88, "reason": "directly addresses value × feasibility framing"},
    {"candidate_id": "def-2", "relevance": 71, "reason": "covers grain selection, peripheral to ranking"}
  ]
}
```

Write to `/tmp/scores.json`.

**Step 4 — Phase 2 finalize.**

```bash
.claude/skills/ask-corpus/.venv/bin/python \
  .claude/skills/ask-corpus/scripts/corpus_research.py \
  --phase=finalize \
  --shortlist /tmp/.../shortlist.json \
  --rerank-scores /tmp/scores.json
```

stdout: `{"report": "/tmp/.../report.json"}`

**Step 5 — Read `report.json` and synthesize per the SYNTHESIS TEMPLATE.** Output starts with the badge, has bold lead-ins per claim, inline `(source: "...", scope)` citations, KEY PATTERNS section, engine footer copied verbatim, and an optional invitation. No `Sources:` block at the end (LAW 1).

If you skipped the plan or rerank-scores, the engine emits `plan-fallback` or `rerank-fallback` warnings — surface them in the synthesis under the engine footer.

### Synthesizing warnings

The engine's `warnings` list is your audit trail for the user. Translate each into a short caveat under the engine footer:

- `plan-fallback` → "Used a single-subquery deterministic plan; consider re-asking with a multi-subquery angle for richer retrieval."
- `rerank-fallback` → "Reranker fell back to local heuristic — relevance ranking is approximate."
- `thin-evidence` → "Fewer than 5 candidates surfaced; the corpus may not cover this question well."
- `scope-concentration` → "Top results all came from one scope; consider widening the scope."
- `scope-errors` → "One or more scopes failed retrieval. Coverage is partial."
- `no-usable-items` → "The corpus returned no usable citations. Falling back to first-principles answer."

---

## LAW ANCHORS

Each LAW is anchored to a specific incident. As new incidents surface, add a new LAW here — the discipline that makes anchors load-bearing is **earning** them, not copying them.

| LAW | Anchor |
|-----|--------|
| LAW 1 — No `Sources:` block | Architecture-driven; carried from `last30days` SKILL.md. The engine emits citations inline so the WebSearch reminder doesn't apply. |
| LAW 2 — Engine footer pass-through | Mechanical. Preserves trace fidelity for the user. |
| LAW 3 — Always double-check | **2026-04-19 mart-naming incident.** First-principles reasoning was directionally correct but missed three actionable specifics the corpus had ready (separate by schema, drop `fct_/dim_` in marts, fewer wider marts over many narrow ones). Without the double-check, the rule would have shipped incomplete. |

---

## WHEN TO USE / SKIP

**Use `ask-corpus` before:**
- Writing any `.claude/rules/*.md` file.
- Scaffolding a dbt model, macro, or test pattern that encodes a design choice.
- Authoring a CI/CD workflow file.
- Making a Metabase operational decision (backup, upgrade, dbt-metabase sync, BQ cost tuning).
- Answering a "why do we…" or "how should I…" architecture question.
- Answering a "what did we decide about X for this client?" question.

**Skip when:**
- The question is pure code-mechanic ("what columns does this model have?") — read the code.
- The answer is already in an existing `.claude/rules/*.md` file — that's already corpus-grounded.
- The topic is unrelated to data engineering / dbt / MDS / Metabase / this engagement.
- The user has already invoked the skill this turn for the same question.

---

## COST NOTE

- `nlm` retrieval calls are **free** (no Pro-search quota; use liberally).
- The planner LLM call is **you** (the host LLM) — no extra API quota.
- The reranker LLM call is **you** (the host LLM) — same.
- Engine I/O is local subprocess + JSON files in `$TMPDIR`.

There is no cost to invoking `ask-corpus` other than the small latency of the two-phase handshake. Default to using it when in doubt.
