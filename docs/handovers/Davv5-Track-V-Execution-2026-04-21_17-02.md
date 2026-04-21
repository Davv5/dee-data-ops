# Session Handover — Track V: Metabase Craft NotebookLM notebook created (14 sources)

**Branch:** `Davv5/Track-V-Metabase-Craft-Notebook`
**Timestamp:** `2026-04-21_17-02` (local)
**Author:** track-executor agent (Claude Sonnet 4.6)
**PR:** not yet opened

---

## Session goal

Create a new NotebookLM notebook named **"Metabase Craft"** on David's account and populate it with 14 high-signal sources covering Metabase OSS self-host (install, ops, dev, warehouse integration, AI, licensing, cost gotchas). Returned `notebook_id` gets recorded in WORKLOG so Track T (corpus config decouple) can wire it into `.claude/corpus.yaml` when that track fires.

## Changed files

```
WORKLOG.md                                                — edited — dated 2026-04-21 entry, includes notebook_id + per-URL status + sanity query
docs/handovers/Davv5-Track-V-Execution-2026-04-21_17-02.md — created — this handover
```

No other repo files changed. Primary deliverable is the external NotebookLM artifact.

## Commands run / run IDs

NotebookLM MCP calls (all returned `status=success`):

- `mcp__notebooklm-mcp__notebook_list` — pre-flight check: no "Metabase Craft" collision (closest match = "Data Ops" + "D-DEE Engagement Memory")
- `mcp__notebooklm-mcp__notebook_create` → `notebook_id: ce484bbc-546b-4fe4-a7db-bc01b847dbe5` (URL: https://notebooklm.google.com/notebook/ce484bbc-546b-4fe4-a7db-bc01b847dbe5)
- `mcp__notebooklm-mcp__source_add` × 14 — all `status=success`, source_ids captured below
- `mcp__notebooklm-mcp__notebook_get` → `source_count=14` confirmed
- `mcp__notebooklm-mcp__notebook_describe` → AI summary spans all four topic pillars (licensing, Docker ops, AI/Metabot, BigQuery integration)
- `mcp__notebooklm-mcp__notebook_query` → sanity test (see below)

### Per-URL upload status (14/14 success)

| # | Category | URL | Source ID | Status |
|---|---|---|---|---|
| 1 | Install+ops | `…/running-metabase-on-docker` | `6efc1a04-a309-4ca7-b855-be88444ab5f8` | success |
| 2 | Install+ops | `…/cloud/cloud-vs-self-hosting` | `d135221d-0453-464e-b00f-0222339d3bed` | success |
| 3 | Install+ops | `…/backing-up-metabase-application-data` | `56b12aa0-e1fc-4964-ae65-47074deb47c7` | success |
| 4 | Install+ops | `…/upgrading-metabase` | `cb79d19e-0a2c-4a8d-af39-29d2a3889a7e` | success |
| 5 | Developer | `…/developers-guide/start` | `a365d384-4563-451b-b67d-6acd1bfea282` | success |
| 6 | Developer | `…/developers-guide/build` | `fcce667d-57bb-456e-86d3-f2d4c02c2cd4` | success |
| 7 | Developer | `…/developers-guide/dev-branch-docker` | `2fd23ff6-6ea9-494a-9228-7973ebe9930e` | success |
| 8 | Warehouse | `…/databases/connections/bigquery` | `20f1b439-f5a7-488a-a823-c155136e51f1` | success |
| 9 | Warehouse | `github.com/gouline/dbt-metabase` | `46e89a29-d013-4d38-b3eb-1ce5bcdda51d` | success (rendered as GitHub repo page — fallback to `README.md` not needed) |
| 10 | AI | `…/ai/metabot` | `85188815-b02e-49a2-a75b-9cbf6be9c4f0` | success |
| 11 | AI | `…/releases/metabase-60` | `d36dd1b9-2e44-41b5-a6de-e2a697f756bb` | success |
| 12 | Licensing | `…/license/` | `4d3656ad-6ad9-4138-8dbd-404fd5f2c3a9` | success |
| 13 | Licensing | `…/license/agpl` | `55da5439-09c2-404f-afaf-af0ecb5164ae` | success |
| 14 | Field notes | `kevinleary.net/blog/bigquery-cost-speed-optimization` | `575650d8-10eb-440d-88cf-64fd66c72b7e` | success |

No failed uploads — all 14 sources processed. Uploads ran in the 14-source category order specified in the track spec so the source pane stays readable by topic block.

### Sanity-test query

**Query:** *"What is the recommended way to back up Metabase's application database when running on Docker with an external Postgres instance?"*

**Answer (summary):** Follow PostgreSQL's standard backup procedure — Metabase stores all runtime app data (questions, dashboards, collections) in a single SQL database, so a standard `pg_dump`-style backup of that database is sufficient for full restore (including during upgrades).

**Citations:** both citations resolved to source_id `56b12aa0-e1fc-4964-ae65-47074deb47c7` — the "Backing up Metabase | Metabase Documentation" source. Matches the track's sanity-criterion ("response cites the backup doc").

## Decisions made

- **Created the notebook unconfirmed** — the `notebook_create` tool signature has no `confirm` param (optional `title` only), so the "confirm=True" stop-and-ask gate didn't apply.
- **Uploaded `github.com/gouline/dbt-metabase` directly** rather than the `/blob/main/README.md` fallback. *Why:* the direct repo URL was accepted by `source_add` and rendered with the expected title ("GitHub - gouline/dbt-metabase: dbt + Metabase integration · GitHub"), so the fallback was unnecessary.
- **No manual source reordering after upload.** *Why:* NotebookLM sorts the source pane alphabetically, so category blocks don't actually preserve visual order. The upload-order-by-category was still followed in case NotebookLM changes that behaviour, but the track doesn't hinge on pane order.

## Unresolved risks

- [ ] Track T hasn't wired the new `notebook_id` into `.claude/corpus.yaml` yet — Metabase questions will still route to the "Data Ops" notebook until that lands. Owner: whichever track-executor picks up Track T next.
- [ ] No audio overview / Studio artifact generated for the Metabase Craft notebook (out of scope for Track V, could be added later as an onboarding touch).

## First task for next session

**Wire `ce484bbc-546b-4fe4-a7db-bc01b847dbe5` into `.claude/corpus.yaml`** (Track T) so rules and the `ask-corpus` skill can route Metabase-topic questions to the Metabase Craft notebook instead of defaulting to the Data Ops notebook. Add a short paragraph in `.claude/rules/using-the-notebook.md` noting the Metabase Craft notebook exists and when to prefer it (Metabase self-host, install, upgrade, license, Metabot, BigQuery-via-Metabase).

## Context links

- NotebookLM notebook URL: https://notebooklm.google.com/notebook/ce484bbc-546b-4fe4-a7db-bc01b847dbe5
- WORKLOG entry: see `WORKLOG.md` 2026-04-21 dated entry
- Existing Data Ops notebook (unchanged): `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`
- Existing D-DEE Engagement Memory notebook (unchanged): `741d85c6-39a7-4612-af7c-cca65043cf19`
- Two-notebook pattern doc: `.claude/rules/using-the-notebook.md`
