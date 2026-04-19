---
paths: ["**/*"]
---

# Using the Data Ops Notebook

This project is paired with a NotebookLM notebook containing 50+ expert sources on data engineering, dbt, modeling, CI/CD, and the modern data stack.

- **Notebook:** https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a
- **Notebook ID:** `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`

## Query the notebook first

Before writing any of the following, invoke the `ask-corpus` skill (or call `mcp__notebooklm-mcp__notebook_query` directly):

- A new `.claude/rules/*.md` file defining a convention
- A dbt macro, model scaffold, or test pattern that encodes a design choice
- A CI/CD workflow file (GitHub Actions, deploy scripts)
- An answer to the user's "why do we…" or "how should I…" question about data architecture

Grounding rules in the notebook prevents invented conventions from drifting into the project.

## Always double-check before finalizing

Even when you have a reasoned answer from first principles, **query the notebook again before locking advice into a rule, model, scope doc, or client-facing deliverable.** The corpus frequently sharpens directionally-right answers with specific patterns that would otherwise be missed.

**Example — the mart-naming rule in this repo (2026-04-19):**

1. User asked whether to build one unified dashboard or many per audience.
2. Claude answered from first-principles reasoning: multiple dashboards, one shared mart layer underneath. Directionally correct.
3. User asked Claude to double-check with the corpus.
4. The corpus confirmed the direction **and added three specifics Claude had not emphasized:**
   - Separate by *schema*, not just by dashboard — so audience-level permissions are enforced at the warehouse
   - Drop `fct_` / `dim_` prefixes in the marts layer — business-friendly names beat Kimball technical names for client-facing tables
   - Fewer, wider marts over many narrow ones — explicit warning against 1:1 mart-per-report
5. Those three findings became Rules 1, 2, and 5 in `.claude/rules/mart-naming.md`.

Without the double-check, the rule would have shipped missing the most actionable specifics. The reasoned answer wasn't wrong — it was incomplete in a way that would have cost the client clarity.

**The default:** whenever you're about to write a `.claude/rules/*.md` file, commit text to a scope or design doc, or make an architectural recommendation to the user, run one more `notebook_query` first — even (especially) when you think you already know the answer. Speed of typing is not a reason to skip a free call.

## Cite the source inline

When a rule or model is informed by the notebook, embed the source title in the file so the convention stays traceable:

```markdown
- Staging models are 1:1 with source tables and materialized as views
  (source: "How to Create a Data Modeling Pipeline (3 Layer Approach)", Data Ops notebook)
```

## When NOT to query the notebook

- Pure code-mechanic questions (what columns does this model have?) — read the code
- Questions already answered by existing `.claude/rules/*.md` — those are already grounded
- Topics unrelated to data engineering / dbt / MDS — out of scope

## Keep the corpus in sync

This is **automatic** via a `PostToolUse` hook in `.claude/settings.json`. Whenever a file under `.claude/rules/*.md` is created or edited with the `Write` or `Edit` tool, the hook runs `.claude/scripts/sync-rule-to-notebook.sh`, which upserts the file into the Data Ops notebook as a text source titled `.claude/rules/<filename>.md`. No manual `source_add` call is needed.

Sync log: `/tmp/dataops-sync-rule.log`. If a sync fails, check that log; the hook is async and does not block the turn.

**If you edit a rule file outside Claude Code** (manual editor, `vim`, etc.), the hook doesn't fire — run the script by hand:

```bash
echo '{"tool_input":{"file_path":"<abs path to rule>"}}' | .claude/scripts/sync-rule-to-notebook.sh
```

## Cost

`notebook_query` is free (no Perplexity / Pro quota). Use it liberally instead of guessing.
