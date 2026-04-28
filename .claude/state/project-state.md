# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-27 (Gold-layer roadmap landed; Phase A complete; merge with ask-corpus v2 from 2026-04-26)_

## Where we are

- **Strategic Reset / Discovery Sprint Phase A: COMPLETE** ~11 days ahead of 2026-05-08 target. Final artifact: `docs/discovery/gold-layer-roadmap.md`. Reviewed and approved by David 2026-04-27.
- **Phase B (Layer Build) reactivation imminent.** Architecture pattern proven for one playbook chapter (Funnel — Speed-to-Lead, shipped 2026-04-23). Roadmap ranks the next 5 chapters by value × feasibility.
- **Mart architecture locked:** **one wide mart per playbook chapter** (`coverage-matrix.md` "Mart architecture commitment" cites `mart-naming.md` Rule 2). Grain split only when justified.
- **Owner model locked:** roles inferred from playbook chapter (SDR Manager / Sales Manager / Marketing Lead / Finance Lead / Sales Operations / D-DEE Leadership), not named individuals — team is too large for human-level routing.
- **Methodology investment landed (2026-04-26):** `ask-corpus` v2 corpus research engine (planner / fan-out / fuse / rerank) merged onto the `chore/triage-2026-04-23` branch via PR #74. 11/13 active units complete; 126/126 tests pass. Plan: `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md`. The separate feature branch `Davv5/Understanding-NotebookLM` is the design-review track and remains open.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 13 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped. `speed_to_lead_detail` mart feeds 15/15 Metabase cards.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` U4a+ resumes when the trusted-GHL-copy decision lands.
- **Current branch:** `chore/triage-2026-04-23` in `/Users/david/Documents/data ops`. Synced with origin via merge commit (Phase-A wrap × ask-corpus v2). AGENTS.md + `.agents/skills/` now tracked for Codex parity; `.obsidian/`, `.cabinet-meta`, `.repo.yaml` gitignored.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **Primary (sprint):** `docs/plans/2026-04-24-strategic-reset.md` — Phase A deliverables done. Phase B (Layer Build) cadence reactivates.
- **Methodology (just landed):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — 11/13 active units `[x]`. U13 (extra fixtures) + U15 (worklog wrap) are the remaining tail.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U1–U3 complete; U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-27** — Gold-layer roadmap landed; Phase A complete. 7 marts ranked across 3 tiers; `speed_to_lead_detail` extends in place; `funnel_booking_detail` is Tier B gated on GHL trusted-copy; Fanbasis staging is the highest-leverage unlock for three Tier-C marts. Owners modelled as roles, not names. (`grep -n "Gold-layer roadmap landed" WORKLOG.md`)
- **2026-04-26** — ask-corpus v2 corpus research engine: two-phase host-LLM JSON handshake (`--phase=retrieve` / `--phase=finalize`); quality-aware diversity guard (parity floor 0.6); 3 LAWs at launch (mart-naming = LAW 3); SKILL.md v2 voice contract. (`grep -n "ask-corpus v2" WORKLOG.md`)
- **2026-04-24** — Strategic Reset: pause new build, run Discovery Sprint, rebuild Gold against ranked roadmap. Foundation sound; problem was visibility + prioritization. (`grep -n "Strategic Reset" WORKLOG.md`)

## Open threads

- **Phase B kickoff candidates** (in order of leverage): (1) extend `speed_to_lead_detail` with Q3 setter columns + Q7 show/no-show columns — additive, parity-test-guarded; (2) Fanbasis staging via `staging-scaffold` against `Raw.fanbasis_transactions_txn_raw` — unblocks three Tier-C marts; (3) `funnel_booking_detail` via `warehouse-fct-scaffold` + `mart-collapse` once GHL trusted-copy decision lands.
- **GHL trusted-copy decision** — single named blocker for Tier B. Choose between legacy blob (1,314 conversation rows) and Phase-2 (101 rows). Resolves 92% undercount + four empty entities + dual-source ambiguity in one move.
- **`Davv5/Understanding-NotebookLM` design-review PR** — three commits ahead of main; open when David approves the v2 design.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` and `QUALITY_PARITY_FLOOR=0.6` ship provisional. Lock after first production queries (3 known-correct-scope questions, top-5 must include the right scope).
- **U13 corpus engine fixtures** — capture `nlm` fixtures (metabase-backup, engagement speed-to-lead) + 3-question smoke variants when David approves the design.
- **`bq-ingest` service repair** — paused per Strategic Reset; GTM-repo work; prerequisite for U4b when cutover resumes.
- **Typeform `form_id` gap** — paused per Strategic Reset; real fix is U9.
- **GHL `messages` / `users` / `tasks`** — 0 rows upstream on both per-object and blob paths; upstream extractor fix out of cutover scope.
- **Fathom transcript landing** — 0% coverage across 1,157 calls; paused per Strategic Reset; fix scheduled for U6 post-resume.
- **Fanbasis** — raw landing fresh; `_fanbasis__sources.yml` placeholder still points at deprecated pre-U2 project; no `stg_fanbasis__*.sql` exists. Highest-leverage staging unlock.
- **Stripe ~50-day staleness** — moot until live revenue questions are answered (Fanbasis is the live source).
- **Re-run roadmap trigger:** GHL trusted-copy decision lands, any 🔴 matrix cell flips to 🟡, or a new business question doesn't fit any existing playbook chapter.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).
- **Agent kit publication** — decide later whether to push `/Users/david/Documents/agent-kit` to GitHub and import as canonical skills source.

## Where to look (retrieval map)

- **Gold-layer roadmap (final discovery artifact):** `docs/discovery/gold-layer-roadmap.md`
- **Mart architecture rule:** `coverage-matrix.md` "Mart architecture commitment" + `.claude/rules/mart-naming.md` Rule 2
- **Corpus engine v2:** `.claude/skills/ask-corpus/scripts/` (engine) + `.claude/skills/ask-corpus/SKILL.md` (voice contract) + `.claude/skills/ask-corpus/SKILL-v1.md` (backup)
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Shared portable kit:** `/Users/david/Documents/agent-kit`
- **Active sprint plan:** `docs/plans/2026-04-24-strategic-reset.md`
- **Corpus engine plan:** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md`
- **Paused cutover plan:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`
- **Sprint artifacts:** `docs/discovery/{source-inventory,source-shapes,staging-models,gap-analysis,insights-summary,business-area-map,coverage-matrix,gold-layer-roadmap}.md`
- **Memories (auto-loaded):** `MEMORY.md` index
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `AGENTS.md` (Codex parity) + `dbt_style_guide.md` + `.claude/rules/*.md`
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks; scope routing in `.claude/rules/using-the-notebook.md`
- **Staging shims (U3, frozen):** `2-dbt/models/staging/{ghl,calendly,stripe,typeform,fathom}/`
- **Existing wide mart (Funnel — Speed-to-Lead):** `2-dbt/models/marts/speed_to_lead_detail.sql`
- **Full history:** `grep -n "^## " WORKLOG.md`; `grep -n "Gold-layer roadmap" WORKLOG.md` for today's wrap
