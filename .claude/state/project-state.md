# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-26 (corpus engine v2 landed on feature branch + worklog wrap)_

## Where we are

- **Active workstream — Strategic Reset / Data Discovery & Visibility Sprint** (2026-04-24 → target 2026-05-08). Pause new build; finish the decision docs that determine the next Gold-layer build. Plan: `docs/plans/2026-04-24-strategic-reset.md`.
- **Phase A methodology investment landed (2026-04-26):** `ask-corpus` v2 corpus research engine (planner / fan-out / fuse / rerank) on branch `Davv5/Understanding-NotebookLM`. 11 of 13 active units complete; 126/126 tests pass. PR not yet opened — awaiting design review. Plan: `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md`.
- **Current operating loop:** `docs/runbooks/operator-fast-loop.md` first, then the deeper Orca cleanup runbook. Start every session from the control-room repo with `.claude/scripts/orca-worktree-audit.sh` + `git status --short --branch`.
- **Foundation is intact (do not rebuild):** BigQuery + dbt + 13 staging models end-to-end + `(id, _ingested_at, payload)` raw-landing discipline. U1 preflight, U2 profile retarget, U3 staging shims all stay shipped.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` resumes (with U5+ warehouse scope rewritten against the Grok roadmap) after the sprint. U4a plumbing parity contract stays valid.
- **Closed workstream (unchanged):** Speed-to-Lead star-schema refactor shipped 2026-04-23. `speed_to_lead_detail` wide mart feeds 15/15 Metabase cards; dashboard live.
- **Next concrete step:** finish `docs/discovery/gold-layer-roadmap.md`. Other discovery docs already exist under `docs/discovery/`.
- **Headline metric (locked 2026-04-19):** unchanged. Stays as the reproduction target for U4a when cutover resumes.

## Active plans

- **Primary (sprint):** `docs/plans/2026-04-24-strategic-reset.md` — 5 workstreams; decision packet under `docs/discovery/`; docs-only until roadmap exists.
- **Methodology (just landed):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — 11 of 13 active units `[x]`; U9 + U10 deferred. U13 (extra fixtures) + U15 (this entry) are the remaining tail.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — 15 units, U1–U3 complete; U4a+ resumes after Gold-layer roadmap exists.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-26** — ask-corpus v2 corpus research engine: two-phase host-LLM JSON handshake (`--phase=retrieve` / `--phase=finalize`); quality-aware diversity guard (parity floor 0.6); 3 LAWs at launch (mart-naming = LAW 3); SKILL.md v2 voice contract; routing rule retargets v1 inline snippet → v2 engine. (`grep -n "ask-corpus v2" WORKLOG.md`)
- **2026-04-24** — Strategic Reset: pause new build, run Discovery Sprint, rebuild Gold against Grok-prioritized roadmap. Third restart considered and rejected; foundation is sound; problem is visibility + prioritization, not tech. (`grep -n "Strategic Reset" WORKLOG.md`)
- **2026-04-23 later** — U3 staging shims landed: 4 blob-shims (Stripe/Typeform/Fathom/Calendly) + GHL column rename; kept pre-U3 Calendly filenames; abandoned Track X Fivetran+poller dual-source; `form_id` gap logged for U9. (`grep -n "U3 staging shims" WORKLOG.md`)

## Open threads

- **`Davv5/Understanding-NotebookLM` PR** — feature branch with 3 commits ahead of main (Phase 1 skeleton + plan doc + Phase 2-4). Open PR when David approves the design.
- **U13 corpus engine fixtures** — capture additional `nlm` fixtures (metabase-backup, engagement speed-to-lead) + 3-question smoke variants. Small nlm spend; defer until design approved.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` and `QUALITY_PARITY_FLOOR=0.6` ship provisional. Acceptance criterion: 3 known-correct-scope questions, top-5 must include the right scope. Lock after first production queries.
- **Sprint discovery packet** — landed: `docs/discovery/{source-inventory,source-shapes,staging-models,gap-analysis,insights-summary,business-area-map,coverage-matrix}.md`; still owed by ~2026-05-08: `docs/discovery/gold-layer-roadmap.md`.
- **Orca/control-room cleanup** — audit shows several worktrees dirty / already merged into `origin/main`; protect dirty work before retiring anything.
- **`merge-dbt-ci@` SA + keyfile** — paused per Strategic Reset; needed before CI can run dbt builds in prod post-resume.
- **`bq-ingest` service repair** — paused per Strategic Reset; GTM-repo work; prerequisite for U4b when cutover resumes.
- **Typeform `form_id` gap** — paused per Strategic Reset; real fix is U9.
- **GHL `conversations` undercount** (101 vs 1,314 blob), **GHL `messages` / `users` / `tasks`** (0 rows upstream), **Fathom transcript landing** (0% across 1,157 calls), **Stripe ~50-day staleness**, **Fanbasis dbt wiring missing** — all paused per Strategic Reset; revisit during cutover resume.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).
- **Agent kit publication/import** — decide later whether to push `/Users/david/Documents/agent-kit` to GitHub and re-import this project from that canonical source.

## Where to look (retrieval map)

- **Corpus engine v2 (just landed):** `.claude/skills/ask-corpus/scripts/` (engine) + `.claude/skills/ask-corpus/SKILL.md` (voice contract) + `.claude/skills/ask-corpus/SKILL-v1.md` (backup)
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Shared portable kit:** `/Users/david/Documents/agent-kit` (installed globally via `~/.claude/agents/data-engineer.md` symlink + 16 `~/.claude/skills/<kit-name>` symlinks; project-imported via `import-agent-kit.sh --symlink`)
- **Data-engineer agent + LAW skills:** `~/.claude/agents/data-engineer.md` owns engagement lifecycle; specialist seams via `altimate-{sql-review,data-parity,schema-migration,dbt-unit-tests}` skills. Discoverability rule: `.claude/rules/use-data-engineer-agent.md`. Hooks: PreToolUse(Write|Edit) → `pre-sql-altimate-review.sh`; PostToolUse(Write|Edit) → `post-sql-qa-baseline.sh`.
- **Orca cleanup protocol:** `docs/runbooks/orca-worktree-power-user-workflow.md`
- **Active sprint plan:** `docs/plans/2026-04-24-strategic-reset.md`
- **Corpus engine plan:** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md`
- **Paused cutover plan:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`
- **Sprint artifacts:** `docs/discovery/`
- **Memories (auto-loaded):** see `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md`
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks; scope routing in `.claude/rules/using-the-notebook.md`
- **Staging shims (U3, frozen):** `dbt/models/staging/{ghl,calendly,stripe,typeform,fathom}/`
- **GTM source repo (read-only reference):** `/Users/david/Documents/operating system/Projects/GTM lead warehouse`
- **Full history:** `grep -n "^## " WORKLOG.md`; `grep -n "ask-corpus v2" WORKLOG.md` for the corpus engine entry
