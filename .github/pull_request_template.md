## Summary
<what changed + why>

## Tests added / updated
<dbt tests, singular tests, CI changes>

## Mart impact
<which marts change rows/columns; attribution-quality-flag coverage delta>

## Validation evidence
<paste output of `dbt test --select <mart>` + release-gate result; note row counts vs oracle>

## Cost note
<BQ slot / storage impact; any new nightly cron footprint>

## DataOps checklist
- [ ] `dbt build` green
- [ ] `dbt test` green (including release gates)
- [ ] WORKLOG.md entry appended
- [ ] `/handover` doc produced

🤖 Generated with Claude Code
Co-Authored-By: Claude <noreply@anthropic.com>
