# Session Handover — Track V: build Metabase craft notebook

**Branch:** `Davv5/Track-V-Metabase-Craft-Notebook`
**Timestamp:** `2026-04-21_16-57` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Create a new NotebookLM notebook named **"Metabase Craft"** on David's NotebookLM account and populate it with 14 high-signal sources covering install, ops, dev, warehouse integration, AI, licensing, and cost gotchas for Metabase OSS self-host. The notebook becomes the craft-level (portable) corpus for every Metabase question the project asks — the same role the existing "Data Ops" notebook plays for dbt/MDS questions.

The `notebook_id` returned by the creation call gets recorded in the WORKLOG entry so Track T (corpus config decouple) can wire it into `.claude/corpus.yaml` when that track fires.

## Changed files (expected)

```
WORKLOG.md                                              — edited — dated entry including the new notebook_id
docs/handovers/Davv5-Track-V-Execution-<timestamp>.md   — created — handover doc
```

**No other repo files change.** This track's primary output is an external artifact (the NotebookLM notebook itself). The only durable repo trace is the WORKLOG entry + handover doc recording what was created.

## Tasks (ordered)

- [ ] Read the existing `.claude/rules/using-the-notebook.md` so you understand the two-notebook pattern David already has (Data Ops + D-DEE Engagement Memory)
- [ ] Call `mcp__notebooklm-mcp__notebook_list` — confirm a "Metabase" notebook doesn't already exist on the account. If one does, **STOP AND ASK** David before proceeding.
- [ ] Call `mcp__notebooklm-mcp__notebook_create` with name `Metabase Craft`. Capture the returned `notebook_id`.
- [ ] For each of the 14 sources below, call `mcp__notebooklm-mcp__source_add` with `source_type=url` and the URL. If the MCP tool reports a failure for a URL, note it in the report and continue; don't block the whole track on one failed source.
- [ ] After all sources are uploaded, call `mcp__notebooklm-mcp__notebook_describe` on the new notebook and confirm the source count (expect 14).
- [ ] Run one sanity test query against the new notebook: `mcp__notebooklm-mcp__notebook_query` with *"What is the recommended way to back up Metabase's application database when running on Docker with an external Postgres instance?"* — confirm the answer cites the `Backing up application data` source, proving retrieval works.
- [ ] Append a WORKLOG entry documenting:
  - The new `notebook_id`
  - The 14 sources uploaded (with any failures noted)
  - The sanity-test question and a 1-line summary of the returned answer
- [ ] Produce a handover doc at `docs/handovers/Davv5-Track-V-Execution-<timestamp>.md` per `docs/handovers/TEMPLATE.md`
- [ ] Commit locally (WORKLOG.md + handover only; no other files). Do NOT push. Do NOT open PR.

## Source list (14 URLs, 1 notebook)

Upload in this order so NotebookLM's chunking/indexing proceeds logically:

**Install + ops**
1. `https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker`
2. `https://www.metabase.com/docs/latest/cloud/cloud-vs-self-hosting`
3. `https://www.metabase.com/docs/latest/installation-and-operation/backing-up-metabase-application-data`
4. `https://www.metabase.com/docs/latest/installation-and-operation/upgrading-metabase`

**Developer path**
5. `https://www.metabase.com/docs/latest/developers-guide/start`
6. `https://www.metabase.com/docs/latest/developers-guide/build`
7. `https://www.metabase.com/docs/latest/developers-guide/dev-branch-docker`

**Warehouse integration**
8. `https://www.metabase.com/docs/latest/databases/connections/bigquery`
9. `https://github.com/gouline/dbt-metabase` (or `https://github.com/gouline/dbt-metabase/blob/main/README.md` if the MCP prefers a file URL)

**AI / agent integration**
10. `https://www.metabase.com/docs/latest/ai/metabot`
11. `https://www.metabase.com/releases/metabase-60`

**Licensing**
12. `https://www.metabase.com/license/`
13. `https://www.metabase.com/license/agpl`

**Field notes (one third-party exception)**
14. `https://www.kevinleary.net/blog/bigquery-cost-speed-optimization/`

## Decisions already made

- **One notebook, not two or three.** At 14 sources, splitting creates query-routing overhead without improving answer quality. Revisit only if the corpus grows past ~40 sources and retrieval gets muddy.
- **Notebook name: `Metabase Craft`.** Mirrors the "Data Ops" name of the existing methodology notebook; signals portable craft knowledge (as opposed to engagement-specific content).
- **Source order matches categorization.** NotebookLM doesn't require ordering, but uploading in category blocks makes the source pane readable for David when he opens the notebook UI.
- **No PII scrubbing.** Every source is public web content; zero PII concerns.

## Open questions

- What if an MCP tool call fails on a URL? **Pick sensible default**: log the failure in the report, continue uploading the rest, don't block the whole track. David can manually add any failures via the NotebookLM UI.
- What if `notebook_create` requires a `confirm=True` parameter? The MCP instructions note "Tools with confirm param require user approval before setting confirm=True." If so, **STOP AND ASK** David before setting.

## Done when

- A notebook named `Metabase Craft` exists on David's NotebookLM account
- It contains at least 13 of the 14 sources (allow one failure without blocking the track)
- `notebook_describe` confirms the source count
- Sanity-test query returns a cited answer from the backup doc
- WORKLOG entry records the new `notebook_id` + any failures
- Handover doc produced
- Commit sits locally, ready for pr-reviewer

## Context links

- `.claude/rules/using-the-notebook.md` — existing two-notebook pattern
- `docs/handovers/Davv5-Track-T-Corpus-Config-Decouple-2026-04-21_10-04.md` — Track T will consume the `notebook_id` this track produces
- Perplexity convergence thread + my research summary live in the session transcript, not a repo file
- Data Ops notebook ID: `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a` (reference pattern)
- D-DEE Engagement Memory notebook ID: `741d85c6-39a7-4612-af7c-cca65043cf19` (reference pattern)
