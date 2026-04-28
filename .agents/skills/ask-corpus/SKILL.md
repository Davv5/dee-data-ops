---
name: ask-corpus
description: Query the project's NotebookLM corpora for cited answers about data engineering concepts, dbt conventions, modeling patterns (star schema, SCDs, medallion), 3-layer pipelines, CI/CD workflows, modern data stack theory, Metabase operations/integration, and engagement-specific decisions. Use when the user asks "what does the style guide say about...", "how should I structure...", "why do we...", or when Codex is about to scaffold a dbt model, a `.Codex/rules/*.md` file, a macro, or a workflow and wants grounded guidance before writing. Also triggers on "ask the corpus", "check the notebook", "what do the sources say", "ground this in the notebook".
---

# Ask Corpus

Query the project's NotebookLM corpora before writing rules, scaffolding models, or answering design-rationale questions — so conventions are grounded in cited expert material instead of invented on the fly.

> **Notebook routing is declared in `.Codex/corpus.yaml`** (not hardcoded here). To add or swap a notebook, edit that file — no skill change needed.

## Corpora available

Resolved from `.Codex/corpus.yaml` at invocation time. Today that file declares:

- **`methodology.data_ops`** — Data Ops notebook (portable craft: dbt, modeling, CI/CD, MDS starter guides)
- **`methodology.metabase`** — Metabase Craft notebook (install/ops, dbt-metabase, BigQuery cost gotchas, Metabot/MCP, AGPL)
- **`methodology.metabase_learn`** — Metabase Learn notebook (133 metabase.com/learn articles + 16 official YouTube walkthroughs — how-to, SQL, visualization choice, dashboards, permissions, BI transition guides)
- **`engagement`** — D-DEE Engagement Memory (this engagement's history, oracle, scope docs)

## Scope parameter (optional)

Pick the scope that matches the question. If unsure, leave it unset and the skill cross-queries all methodology notebooks.

| scope value                 | what it queries                                                | when to use                                                                 |
| --------------------------- | -------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `methodology.data_ops`        | Data Ops notebook only                                       | dbt / modeling / CI/CD / MDS questions where Metabase isn't in the picture  |
| `methodology.metabase`        | Metabase Craft notebook only                                 | Metabase **ops / integration / licensing**: install, dbt-metabase, BQ cost, AGPL, Metabot |
| `methodology.metabase_learn`  | Metabase Learn notebook only                                 | Metabase **how-to / authoring / SQL**: building questions, dashboard patterns, chart choice, SQL tutorials, BI transition guides |
| `methodology` *(default)*     | Cross-query all methodology notebooks                        | Portable craft questions where you aren't sure which notebook has the answer |
| `engagement`                  | D-DEE Engagement Memory only                                 | "What did we decide about X for this client?" type history questions         |

## How to use (invocation recipe)

**Step 1 — resolve the notebook_id(s) from `.Codex/corpus.yaml`** with this bash+python snippet (pyyaml is in the stdlib-adjacent toolchain on macOS/Linux; works without installing `yq`):

```bash
python3 - "$SCOPE" <<'PY'
import sys, yaml, json, pathlib
scope = (sys.argv[1] or "methodology").strip()
cfg_path = pathlib.Path(".Codex/corpus.yaml")
if not cfg_path.exists():
    # Safety fallback: preserve the old hardcoded Data Ops notebook
    # so partial template adoption doesn't break the skill.
    print(json.dumps({
        "warning": "corpus.yaml missing; falling back to hardcoded Data Ops notebook",
        "notebook_ids": ["7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a"],
        "names": ["Data Ops (fallback)"]
    }))
    sys.exit(0)

c = yaml.safe_load(cfg_path.read_text())
ids, names = [], []

if scope == "engagement":
    ids.append(c["engagement"]["notebook_id"])
    names.append(c["engagement"]["name"])
elif scope.startswith("methodology."):
    key = scope.split(".", 1)[1]
    for m in c.get("methodology", []):
        if m["key"] == key:
            ids.append(m["notebook_id"])
            names.append(m["name"])
            break
    if not ids:
        print(json.dumps({"error": f"unknown methodology key: {key}"}))
        sys.exit(2)
elif scope in ("methodology", ""):
    for m in c.get("methodology", []):
        ids.append(m["notebook_id"])
        names.append(m["name"])
else:
    print(json.dumps({"error": f"unknown scope: {scope}"}))
    sys.exit(2)

print(json.dumps({"notebook_ids": ids, "names": names}))
PY
```

Pass `$SCOPE` as one of: `methodology.data_ops`, `methodology.metabase`, `methodology`, `engagement`, or empty. The output is a JSON object with `notebook_ids` (list) and `names` (list).

**Step 2 — fire the MCP query.** If one id came back, call `notebook_query`. If multiple, call `cross_notebook_query` across all of them.

```
# single notebook
mcp__notebooklm-mcp__notebook_query(
  notebook_id="<from step 1>",
  query="<the user's question, rephrased for retrieval>"
)

# multiple notebooks (default methodology scope with >1 notebook)
mcp__notebooklm-mcp__cross_notebook_query(
  notebook_ids=[<list from step 1>],
  query="<the user's question, rephrased for retrieval>"
)
```

**Step 3 — format the response:**

1. **TL;DR** — 1-2 sentence direct answer
2. **What the sources say** — bullets prefixed with the source title, each tagged with which notebook it came from (e.g., `[Data Ops]`, `[Metabase Craft]`, `[D-DEE]`)
3. **Notebook link(s):** https://notebooklm.google.com/notebook/<id> for each notebook queried

When Codex writes a rule or model file **after** this query, embed the source title inline as justification so the convention is traceable:

```markdown
- Staging models are 1:1 with source tables and materialized as views
  (source: "How to Create a Data Modeling Pipeline (3 Layer Approach)", Data Ops notebook)
```

## When to use

**Use it:**
- Drafting a new `.Codex/rules/*.md` convention file
- Scaffolding a dbt model, macro, test, or CI workflow and want grounded defaults
- User asks "why do we…", "how should I…", "what's the right way to…" about data architecture or Metabase ops
- User says "ask the corpus" / "check the notebook" / "ground this"
- Engagement-specific questions: "what did we decide about X for D-DEE?"

**Skip it:**
- Pure code-mechanic questions (what columns does this file have?) — just read the file
- Question already answered inside `.Codex/rules/` — those are already grounded
- Topic outside data engineering / dbt / MDS / Metabase / engagement history

## Example (scoped)

**User:** "what's the recommended way to back up Metabase's application database?"

**Codex:**
1. Runs the Step 1 snippet with `SCOPE=methodology.metabase` → gets the Metabase Craft `notebook_id`
2. Calls `notebook_query(notebook_id=<metabase id>, query="application database backup strategy")`
3. Replies with TL;DR + bulleted sources + notebook link

## Fallback safety

If `.Codex/corpus.yaml` is missing (e.g., someone forked the template and hasn't added one yet), the Step 1 snippet returns the Data Ops notebook id as a fallback and emits a one-line warning. The skill keeps working; just without multi-notebook routing.

## Cost note

`notebook_query` and `cross_notebook_query` are free — no Perplexity quota, no per-call cost. Use them liberally rather than guessing. If the query returns no useful citations, say so and fall back to reasoning from first principles.
