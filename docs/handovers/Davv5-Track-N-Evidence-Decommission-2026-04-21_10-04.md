# Session Handover — Track N: Evidence decommission

**Branch:** `Davv5/Track-N-Evidence-Decommission`
**Timestamp:** `2026-04-21_10-04` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Remove Evidence.dev from the repo now that the engagement has pivoted to self-hosted Metabase. Archive the mockup branch's workflow and delete the Evidence-specific code, keeping the Looker Studio click-spec and oracle seeds intact because they still ground the Metabase dashboards.

## Changed files (expected)

```
dashboards/evidence/                           — deleted (whole dir)
.github/workflows/evidence-preview.yml         — deleted
WORKLOG.md                                     — edited — append dated entry
```

After this track lands, the remote branch `mockup/evidence-preview` can be deleted by David at his discretion — do NOT delete remote branches from the executor.

## Tasks

- [ ] Confirm `main` has no references to `dashboards/evidence/` or the evidence-preview workflow (sanity check; should already be the case — Evidence work lived only on `mockup/evidence-preview`)
- [ ] Verify the branch name matches the track (rename with `git branch -m` if the worktree came up under a different branch)
- [ ] `git rm -r dashboards/evidence/` if present on this branch
- [ ] `git rm .github/workflows/evidence-preview.yml` if present on this branch
- [ ] Grep the repo for remaining `evidence` references (case-insensitive). Any hits in `.claude/rules/*.md`, `README.md`, `dbt_style_guide.md`, `v1_build_plan.md`, `client_v1_scope_speed_to_lead.md` get cleaned up in the same commit. Hits in `WORKLOG.md` entries stay put — that's history.
- [ ] Append a WORKLOG.md entry per `.claude/rules/worklog.md`
- [ ] Run `/handover` slash command to produce the post-execution handover doc
- [ ] Commit locally (do not push — pr-reviewer handles push + PR)

## Decisions already made

- **Keep Looker Studio click-spec + oracle seeds.** The Looker work (Track H) still has reference value for Metabase dashboard layout. Do not delete `docs/looker-studio/*` or `dbt/models/looker/*` or any `dbt/seeds/validation/oracle_*.csv`.
- **Do NOT delete remote branch `mockup/evidence-preview`.** David makes that call — it may be worth keeping as a comparison artifact for a week or two.
- **Do NOT touch `.github/workflows/dbt-*.yml`** — those are Track G infra, not Evidence.
- **Do NOT touch GCP Pages / gh-pages branch.** Whatever was published there stays; Metabase serves from GCE, not GH Pages.

## Open questions

None expected. If the executor finds an Evidence reference in an unexpected place (e.g., a mart comment referencing Evidence as the consumer), pick the sensible default: rewrite the comment to reference Metabase, and note in commit.

## Done when

- `find . -path ./node_modules -prune -o -type d -name evidence -print` returns nothing under tracked paths
- `rg -l evidence` returns zero non-WORKLOG hits (case-insensitive)
- `git status` is clean after commit
- WORKLOG entry appended
- Handover doc written

## Context links

- Scope decision: WORKLOG.md entry "2026-04-21 — Dashboard pivot: Metabase OSS self-host on GCP"
- Metabase scaffold PR: #34 (`feat/metabase-self-host`)
- `.claude/rules/metabase.md` — the replacement convention
