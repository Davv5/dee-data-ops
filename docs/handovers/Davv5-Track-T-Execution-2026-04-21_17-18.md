# Session Handover — Track T: corpus config decouple (ask-corpus skill + `.claude/corpus.yaml`)

**Branch:** `Davv5/Track-T-Corpus-Config-Decouple`
**Timestamp:** `2026-04-21_17-18` (local)
**Author:** track-executor agent (Claude)
**PR:** not yet opened (local commit only per track-executor contract)

---

## Session goal

Decouple the `ask-corpus` skill from hardcoded D-DEE notebook IDs so it becomes portable across PS engagements. A per-project `.claude/corpus.yaml` declares which NotebookLM notebooks the project uses and routes queries by scope (`methodology.data_ops`, `methodology.metabase`, `methodology` cross-query default, `engagement`). Future clients get a fresh `corpus.yaml`; the skill itself doesn't change.

## Changed files

```
.claude/corpus.yaml                     — created — 3 notebooks declared, methodology as LIST
.claude/skills/ask-corpus/SKILL.md      — edited — reads corpus.yaml, scope param, fallback
.claude/rules/using-the-notebook.md     — edited — 3 routing modes + "add a new methodology notebook"
CLAUDE.md                               — edited — added "Corpus config" pointer section
WORKLOG.md                              — edited — dated entry with 3 test query results
docs/handovers/Davv5-Track-T-Execution-2026-04-21_17-18.md  — created — this file
```

## Commands run / run IDs

- `python3 -c "..."` — manually exercised the Step 1 resolver snippet against all 3 scopes (`methodology.data_ops`, `methodology.metabase`, `engagement`). All returned correct notebook_id + name.
- `mcp__notebooklm-mcp__notebook_query` — 3 end-to-end test calls, one per scope:
  1. Metabase Craft (`ce484bbc-546b-4fe4-a7db-bc01b847dbe5`) — "backup Metabase app DB?" — cited "Backing up Metabase" doc.
  2. Data Ops (`7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`) — "canonical 3-layer dbt architecture?" — cited "How to Create a Data Modeling Pipeline (3 Layer Approach)" + warehouse/marts rule files.
  3. D-DEE Engagement Memory (`741d85c6-39a7-4612-af7c-cca65043cf19`) — "Speed-to-Lead metric grain?" — cited sorted-rabbit plan confirming Calendly-event grain (3,141 bookings).

No dbt or GH Actions runs — this track is skill/config refactor only.

## Decisions made

- **`methodology` is a LIST, not a single dict.** New craft notebooks register by adding a `{key, notebook_id, name, purpose}` entry — no skill code change. Originally Track T's track file described a single-dict schema; David's Metabase notebook made a 2nd methodology notebook concrete, so the schema generalized.
- **Default scope = `methodology` cross-query** (not engagement). "How should I structure X?" type questions hit craft first; engagement scope is narrower and more intentional.
- **python+pyyaml over bash+yq** for the Step 1 resolver. pyyaml is already in the dbt-bigquery venv on this machine; `yq` isn't a hard dep.
- **Fallback to hardcoded Data Ops id if `corpus.yaml` missing.** Template forks that haven't added a `corpus.yaml` shouldn't break the skill.
- **Skill name unchanged.** `ask-corpus` stays, only internals changed.

## Unresolved risks

- [ ] None that block further work. The three agent personas (architect/executor/reviewer) will pick up scope routing automatically on their next invocation via this skill.
- [ ] `pyyaml` is assumed present — if a future environment lacks it, the Step 1 snippet exits non-zero with a JSON error. Mitigation would be to document `pip install pyyaml` in the skill's README or to add a fallback JSON-via-`yq` path. Low priority; every dev environment that runs dbt already has pyyaml via dbt-core.

## First task for next session

**One actionable item:** Open PR `Davv5/Track-T-Corpus-Config-Decouple → main` with the commit from this session, wait for review, and merge. After merge, verify the three agent personas (`.claude/agents/architect.md`, `track-executor.md`, reviewer) can resolve notebook IDs via `corpus.yaml` without adjustment.

## Context links

- Track source: `docs/handovers/Davv5-Track-T-Corpus-Config-Decouple-2026-04-21_10-04.md` (on PR #35 branch, not yet on main — schema in that file is outdated; final schema lives in `.claude/corpus.yaml`)
- NotebookLM notebooks referenced (not modified):
  - Data Ops: https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a
  - Metabase Craft: https://notebooklm.google.com/notebook/ce484bbc-546b-4fe4-a7db-bc01b847dbe5
  - D-DEE Engagement Memory: https://notebooklm.google.com/notebook/741d85c6-39a7-4612-af7c-cca65043cf19
- Worklog entry: `WORKLOG.md` §2026-04-20 "Track T: corpus config decouple"
