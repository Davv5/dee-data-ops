---
paths: ["**/*"]
---

# Using the Project Notebooks

This project is paired with multiple NotebookLM notebooks — portable craft knowledge plus this engagement's history. The authoritative list of notebooks and their notebook_ids lives in **`.claude/corpus.yaml`**, not hardcoded in this rule or in the `ask-corpus` skill.

## Corpora declared in `.claude/corpus.yaml`

- **`methodology.data_ops`** — Data Ops notebook. 50+ expert sources on dbt, modeling, CI/CD, modern data stack. Portable across clients.
- **`methodology.metabase`** — Metabase Craft notebook. Self-hosted Metabase OSS: install/ops, BigQuery connection + cost gotchas, dbt-metabase integration, Metabot + MCP, AGPL. Portable across clients.
- **`methodology.metabase_learn`** — Metabase Learn notebook. Full metabase.com/learn crawl: 133 articles + 16 official YouTube walkthroughs. How-to/authoring/SQL/visualization — the *end-user analyst* side of Metabase. Portable across clients.
- **`engagement`** — D-DEE Engagement Memory notebook. This engagement's scope docs, oracle metrics, decisions. Disposable when the engagement ends.

To swap or add a notebook, edit `.claude/corpus.yaml`. No code change needed. No rule change needed. The `ask-corpus` skill reads the file at invocation time.

## Query the notebook first

Before writing any of the following, invoke the `ask-corpus` skill (which routes via `corpus.yaml`):

- A new `.claude/rules/*.md` file defining a convention
- A dbt macro, model scaffold, or test pattern that encodes a design choice
- A CI/CD workflow file (GitHub Actions, deploy scripts)
- A Metabase operational decision (backup, upgrade, dbt-metabase sync, BQ cost tuning)
- An answer to the user's "why do we…" or "how should I…" question about data architecture or Metabase ops
- An answer to "what did we decide about X for this client?" (engagement scope)

Grounding answers in the corpus prevents invented conventions from drifting into the project.

## Which scope to use

Pick the `scope` that matches the question. When in doubt, leave scope unset — the default (`methodology`) cross-queries all craft notebooks.

| scope value                   | covers                                                                                                |
| ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `methodology.data_ops`        | dbt conventions, modeling, warehouse design, CI/CD, MDS starter guides                                |
| `methodology.metabase`        | Metabase **ops / integration / licensing**: install, Cloud SQL backup, upgrade, dbt-metabase, BQ cost, AGPL, Metabot |
| `methodology.metabase_learn`  | Metabase **how-to / authoring / SQL**: how to build a dashboard or question, which chart to pick, drill-through, SQL tutorials, BI-tool transition guides |
| `methodology` *(default)*     | Cross-query all craft notebooks when you aren't sure which has the answer                             |
| `engagement`                  | D-DEE history: scope decisions, oracle numbers, prior client conversations                            |

Rule of thumb:

- Writing a portable `.claude/rules/*.md`? Use `methodology` (default) so a Metabase rule stays cross-checked against Data Ops conventions and vice versa.
- Writing an *operational* Metabase file (install doc, connector config, dbt-metabase YAML, Cloud SQL backup plan)? Use `methodology.metabase`.
- Writing an *authoring* Metabase decision (dashboard tile, visualization choice, filter wiring, SQL question pattern)? Use `methodology.metabase_learn`.
- Unsure which Metabase notebook applies? Use `methodology` — it cross-queries both plus Data Ops for free.
- Writing a D-DEE-only mart, oracle reconciliation, or client-facing deliverable? Use `engagement` to ground it in what was already decided.
- Cost is zero; over-querying is fine.

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

When a rule or model is informed by the corpus, embed the source title in the file so the convention stays traceable. Also tag which notebook the source came from when the distinction matters:

```markdown
- Staging models are 1:1 with source tables and materialized as views
  (source: "How to Create a Data Modeling Pipeline (3 Layer Approach)", Data Ops notebook)

- Back up the Metabase app DB via Cloud SQL automated backups, not file snapshots
  (source: "Metabase Self-Hosted Operations Guide", Metabase Craft notebook)
```

## Adding a new methodology notebook

When a new craft area becomes relevant (e.g., "dlthub" for ingestion, "great-expectations" for DQ), add a new entry to the `methodology:` list in `.claude/corpus.yaml`:

```yaml
methodology:
  - key: data_ops
    notebook_id: 7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a
    name: Data Ops
    purpose: |
      …
  - key: metabase
    notebook_id: ce484bbc-546b-4fe4-a7db-bc01b847dbe5
    name: Metabase Craft
    purpose: |
      …
  - key: <new_key>                # keep keys snake_case, short, stable
    notebook_id: <new-uuid>
    name: <Human-Readable Name>
    purpose: |
      One-paragraph description of what's in the notebook and when to query it.
```

Then document the new key + scope in this rule's "Which scope to use" table. The `ask-corpus` skill requires no change — it picks up the new entry automatically.

## When NOT to query the notebook

- Pure code-mechanic questions (what columns does this model have?) — read the code
- Questions already answered by existing `.claude/rules/*.md` — those are already grounded
- Topics unrelated to data engineering / dbt / MDS / Metabase / this engagement — out of scope

## Keep the corpus in sync

This is **automatic** via a `PostToolUse` hook in `.claude/settings.json`. Whenever a file under `.claude/rules/*.md` is created or edited with the `Write` or `Edit` tool, the hook runs `.claude/scripts/sync-rule-to-notebook.sh`, which upserts the file into the Data Ops notebook as a text source titled `.claude/rules/<filename>.md`. No manual `source_add` call is needed.

Sync log: `/tmp/dataops-sync-rule.log`. If a sync fails, check that log; the hook is async and does not block the turn.

**If you edit a rule file outside Claude Code** (manual editor, `vim`, etc.), the hook doesn't fire — run the script by hand:

```bash
echo '{"tool_input":{"file_path":"<abs path to rule>"}}' | .claude/scripts/sync-rule-to-notebook.sh
```

## Cost

`notebook_query` and `cross_notebook_query` are free (no Perplexity / Pro quota). Use them liberally instead of guessing.
