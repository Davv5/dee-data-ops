# Session Handover — Track O: stale GH Secrets cleanup

**Branch:** `Davv5/Track-O-Stale-GH-Secrets-Cleanup`
**Timestamp:** `2026-04-21_10-04` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Remove the `GHL_API_KEY` and `GHL_LOCATION_ID` GitHub Actions secrets that were superseded by the GCP Secret Manager migration (Track J, 2026-04-20). Leaving them in place is both a rotation-hygiene risk and a confusion risk for the next operator — they look authoritative but they're stale.

## Changed files (expected)

No file diff expected — this is a GitHub repo-level state change, not a code change. The track produces a WORKLOG entry only.

```
WORKLOG.md   — edited — append dated entry documenting the secret deletions
```

## Tasks

- [ ] Verify the secrets are present in the first place:
      `gh secret list --repo Davv5/dee-data-ops` — expect `GHL_API_KEY` and `GHL_LOCATION_ID` listed
- [ ] Verify the current `ingestion/ghl/extract.py` path and `.github/workflows/ingest.yml` do NOT reference `GHL_API_KEY` or `GHL_LOCATION_ID` as workflow env vars (they should fetch from Secret Manager at runtime post-Track-J)
- [ ] **STOP AND ASK** if either secret is still referenced in any workflow or source file. Do not delete.
- [ ] `gh secret delete GHL_API_KEY --repo Davv5/dee-data-ops`
- [ ] `gh secret delete GHL_LOCATION_ID --repo Davv5/dee-data-ops`
- [ ] Trigger the next scheduled `ingest.yml` run manually with `gh workflow run ingest.yml --repo Davv5/dee-data-ops` and verify it still succeeds (ingest pulls from Secret Manager, so it should be unaffected)
- [ ] Append WORKLOG entry noting the deletion + the verifying workflow run URL
- [ ] Run `/handover`
- [ ] Commit locally (the WORKLOG edit only)

## Decisions already made

- **Deletion, not rotation.** The values themselves aren't compromised today (the exposed PIT from 2026-04-19 was already rotated in Track J). These secrets just became unused. Delete.
- **No dry-run / soft-delete.** `gh secret delete` is immediate. If we need them back we generate new ones — the old values were low-entropy PITs that rotate freely.

## Open questions

- What to do if a workflow IS still referencing one of these? **STOP AND ASK** — that means Track J's migration was incomplete, which is a different scope than this track.

## Done when

- `gh secret list --repo Davv5/dee-data-ops` shows neither secret
- One successful post-deletion workflow run logged in the WORKLOG
- Commit sits locally, ready for pr-reviewer

## Context links

- Track J handover: `docs/handovers/Davv5-Track-J-*.md` (the Secret Manager migration)
- `.claude/rules/ingest.md` — "Security" section: secrets must live in Secret Manager
- Incident log: WORKLOG entry 2026-04-19 — the exposed PIT rotation
