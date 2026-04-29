---
paths: ["**/*"]
---

# Using the Project Notebooks

This project is paired with NotebookLM notebooks for portable craft knowledge plus this engagement's history. The authoritative list of notebooks lives in **`.claude/corpus.yaml`** — not hardcoded here.

## What lives where

- **`.claude/corpus.yaml`** — notebook IDs, names, purposes. Edit this to swap or add a notebook. No code change needed.
- **`.claude/skills/ask-corpus/SKILL.md`** — the v2 voice contract: trigger list, scope routing table, plan generation rules, handshake protocol, synthesis template, the three LAWs, and a worked example.
- **This file** — when to invoke the skill at the project level, the cite-source norm, the auto-sync hook, the boundary with raw MCP / Perplexity, and how to add a new notebook.

## Invoke `ask-corpus` for grounded design / convention / history

Whenever you would otherwise reason from first principles to write a `.claude/rules/*.md` convention, scaffold a dbt model or macro, author a CI/CD workflow, decide a Metabase ops question, answer a "why do we…" / "how should I…" question, or recall "what did we decide about X for this client?" — invoke `ask-corpus` first. The full trigger list and scope routing table live in `SKILL.md`.

**LAW 3:** always double-check the corpus before locking advice into a rule, model, scope doc, or client-facing deliverable — even when first-principles reasoning gives you a directionally-correct answer. The corpus often sharpens directionally-right answers with specific patterns that would otherwise be missed (anchor: 2026-04-19 mart-naming incident).

**Do not call `mcp__notebooklm-mcp__notebook_query` or `cross_notebook_query` directly when this rule applies** — the skill owns the orchestrated path.

## Engine vs raw MCP vs Perplexity

- **`ask-corpus` engine (default for in-corpus questions).** Grounded design, conventions, history. Returns a structured Report with badge, inline citations, warnings footer.
- **Raw `mcp__notebooklm-mcp__notebook_query` (rare).** One-off ad-hoc lookups inside a single notebook when the engine is overkill — e.g., quickly fetching a known source title to add as a citation, or peeking at what a notebook contains. Don't use it for design decisions; that's what the engine exists for.
- **Perplexity (`pplx_smart_query`, etc.).** Current-state external information that isn't in the corpus — vendor docs released yesterday, breaking changes in a tool we use, industry comparisons. Quota-limited; default `intent='quick'` (free Sonar). Never substitute for `ask-corpus` on questions the corpus already covers.

## Cite the source inline

When a rule or model is informed by the corpus, embed the source title in the file so the convention stays traceable. Tag which notebook the source came from when the distinction matters:

```markdown
- Staging models are 1:1 with source tables and materialized as views
  (source: "How to Create a Data Modeling Pipeline (3 Layer Approach)", Data Ops notebook)

- Back up the Metabase app DB via Cloud SQL automated backups, not file snapshots
  (source: "Metabase Self-Hosted Operations Guide", Metabase Craft notebook)
```

### Corpus citation vs empirical anchor — two distinct grounding styles

A rule can be grounded in two different kinds of source. Use the format that matches:

- **Corpus citation** (the form above) — when the source is a NotebookLM notebook entry. Embed the title and tag the notebook. Tells the reader "this rule is methodology-grounded; verify against the corpus if you doubt it."
- **Empirical anchor** — when the source is an in-repo incident, a PR review, a bug, or a specific session's lesson. Use the form `**Empirical anchor (YYYY-MM-DD).** <one-paragraph narrative of what happened, what failed, what cost was paid>`. Tells the reader "this rule was paid for in real failure; the cost is recoverable in the linked PR / WORKLOG entry."

Both can coexist in the same rule when the principle is corpus-grounded *and* the specific shape was sharpened by an incident:

```markdown
> **`QUALIFY ROW_NUMBER() OVER (...) = 1` in staging is a code smell.**
> (source: "[AE] The Order in which I Model Data" — Joshua Kim, Medium "last tip")

**Empirical anchor (2026-04-19).** The mart-naming incident — three rollups
named with non-corpus-canonical suffixes — caused the rule to be split out
of `data-modeling-process.md` into its own `mart-naming.md` after the corpus
sharpened a directionally-right naming intuition.
```

Don't substitute one for the other. Empirical anchors without corpus citation drift toward "things we said once"; corpus citations without empirical anchors are easy to ignore under deadline pressure.

## Adding a new methodology notebook

When a new craft area becomes relevant (e.g., "dlthub" for ingestion, "great-expectations" for DQ), add an entry to `methodology:` in `.claude/corpus.yaml`:

```yaml
methodology:
  - key: <new_key>                # snake_case, short, stable
    notebook_id: <new-uuid>
    name: <Human-Readable Name>
    purpose: |
      One-paragraph description of what's in the notebook and when to query it.
```

Then update the scope routing table in `.claude/skills/ask-corpus/SKILL.md`. The skill picks up the new entry automatically.

## Keep the corpus in sync

Automatic via a `PostToolUse` hook in `.claude/settings.json`. Whenever a file under `.claude/rules/*.md` is created or edited with the `Write` or `Edit` tool, the hook runs `.claude/scripts/sync-rule-to-notebook.sh`, which upserts the file into the Data Ops notebook as a text source titled `.claude/rules/<filename>.md`. No manual `source_add` call is needed.

Sync log: `/tmp/dataops-sync-rule.log`. If a sync fails, check that log; the hook is async and does not block the turn.

**If you edit a rule file outside Claude Code** (manual editor, `vim`, etc.), the hook doesn't fire — run the script by hand:

```bash
echo '{"tool_input":{"file_path":"<abs path to rule>"}}' | .claude/scripts/sync-rule-to-notebook.sh
```

## Cost

`nlm` retrieval is free (no Perplexity / Pro quota). Planner and reranker LLM calls are made by you (the host LLM); no extra API quota beyond your own conversation. Default to using the skill instead of guessing.
