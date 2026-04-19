---
name: ask-corpus
description: Query the "Data Ops" NotebookLM notebook for cited answers about data engineering concepts, dbt conventions, modeling patterns (star schema, SCDs, medallion), 3-layer pipelines, CI/CD workflows, and modern data stack theory. Use when the user asks "what does the style guide say about...", "how should I structure...", "why do we...", or when Claude is about to scaffold a dbt model, a `.claude/rules/*.md` file, a macro, or a workflow and wants grounded guidance before writing. Also triggers on "ask the corpus", "check the notebook", "what do the sources say", "ground this in the notebook".
---

# Ask Corpus

This project is paired with a NotebookLM notebook containing 50+ expert sources on data engineering. Query it **before** writing rules, scaffolding models, or answering design-rationale questions — so conventions are grounded in cited expert material instead of invented on the fly.

**Notebook:** [Data Ops on NotebookLM](https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a)  
**Notebook ID:** `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`

## What's in the corpus

- **Data By Design free course** — 3 modules (ingestion, modeling, workflow)
- **~35 Modern Data Community videos** — star schema, Kimball, SCDs, medallion, 3-env design, CI/CD, deployment, dbt project setup with Claude Code
- **Starter Guide PDFs** — 10 MDS Components, dbt, GitHub, Snowflake, Modern Data
- **Project docs** — this repo's `CLAUDE.md`, `dbt_style_guide.md`, GitHub workflow templates, PR template

## When to use

**Use it:**
- Drafting a new `.claude/rules/*.md` convention file
- Scaffolding a dbt model, macro, test, or CI workflow and want grounded defaults
- User asks "why do we…", "how should I…", "what's the right way to…" about data architecture
- User says "ask the corpus" / "check the notebook" / "ground this"

**Skip it:**
- Pure code-mechanic questions (what columns does this file have?) — just read the file
- Question already answered inside `.claude/rules/` — those are already grounded
- Topic outside data engineering / dbt / MDS

## How to use

Call the MCP tool directly:

```
mcp__notebooklm-mcp__notebook_query(
  notebook_id="7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a",
  query="<the user's question, rephrased for retrieval>"
)
```

Then format the response as:

1. **TL;DR** — 1-2 sentence direct answer
2. **What the sources say** — bullets prefixed with the source title (click-through-able via the notebook link below)
3. **Notebook:** https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a

When Claude writes a rule or model file **after** this query, embed the source title inline as justification so the convention is traceable:

```markdown
- Staging models are 1:1 with source tables and materialized as views
  (source: "How to Create a Data Modeling Pipeline (3 Layer Approach)", Data Ops notebook)
```

## Example

**User:** "what naming convention does the style guide recommend for fact tables?"

**Claude:**
1. Calls `notebook_query(query="naming convention for fact tables dbt")`
2. Gets passages from "Creating a Data Model w/ dbt: Facts (Part 2/3)" and "dbt_style_guide.md"
3. Replies:
   - **TL;DR:** Fact tables use `fct_<business_process>` (e.g., `fct_orders`).
   - **Sources:** "Creating a Data Model w/ dbt: Facts (Part 2/3)" — business-process naming; "dbt_style_guide.md" — `fct_` prefix + plural noun convention.
   - **Notebook:** https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a

## Cost note

`notebook_query` is free — no Perplexity quota, no per-call cost. Use it liberally rather than guessing. If the query returns no useful citations, say so and fall back to reasoning from first principles.
