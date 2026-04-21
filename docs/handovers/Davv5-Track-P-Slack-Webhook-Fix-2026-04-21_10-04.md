# Session Handover — Track P: Track K Slack webhook input fix

**Branch:** `Davv5/Track-P-Slack-Webhook-Fix`
**Timestamp:** `2026-04-21_10-04` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Fix the Slack failure-alert step in the three CI/CD workflows Track K added (`dbt-ci.yml`, `dbt-deploy.yml`, `dbt-nightly.yml`). The step currently fails to deliver when the underlying job fails — a bug in how the `slackapi/slack-github-action` action's webhook input was wired. Alerts are supposed to land in `#dee-dataops-alerts`.

## Changed files (expected)

```
.github/workflows/dbt-ci.yml           — edited — fix Slack step
.github/workflows/dbt-deploy.yml       — edited — fix Slack step
.github/workflows/dbt-nightly.yml      — edited — fix Slack step
WORKLOG.md                              — edited — dated entry
```

## Tasks

- [ ] Open each of the three workflow files and locate the Slack failure-notification step. It looks roughly like:
      ```yaml
      - name: Notify Slack on failure
        if: failure()
        uses: slackapi/slack-github-action@v1
        with:
          payload: ...
      ```
- [ ] Identify which input shape is being used. The action supports two modes:
      - **Webhook mode** — uses `payload:` + requires `SLACK_WEBHOOK_URL` env var + `webhook-type: incoming-webhook`
      - **API mode** — uses `channel-id:` + `slack-message:` + requires `SLACK_BOT_TOKEN`
      David's config uses the webhook approach (`SLACK_WEBHOOK_URL` secret). Ensure each step includes:
      ```yaml
      env:
        SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
        SLACK_WEBHOOK_TYPE: INCOMING_WEBHOOK
      ```
      AND uses `webhook-type: incoming-webhook` in the `with:` block.
- [ ] After editing, run `gh workflow run dbt-nightly.yml --ref Davv5/Track-P-Slack-Webhook-Fix` to trigger a known-failing test (add a temporary `exit 1` in the dbt step, or use `--select nonexistent_model` to force failure), wait for the run, confirm Slack alert lands
- [ ] Remove the intentional-failure bait commit
- [ ] Re-run the workflow to confirm green path doesn't alert
- [ ] Append WORKLOG entry with the workflow run URLs
- [ ] Run `/handover`
- [ ] Commit locally

## Decisions already made

- **Webhook mode, not API mode.** David's Track K shipped with `SLACK_WEBHOOK_URL`; keep that pattern. Don't flip to bot tokens.
- **Channel target is `#dee-dataops-alerts`.** Configured on the Slack side via the webhook; not passed as an input.
- **No retry logic.** Slack webhook failures are rare and non-fatal. Do not add retry layers.

## Open questions

- What's the exact failure mode the current step exhibits? If the executor confirms the webhook input is the problem, fix it. If the failure is something else (e.g., `SLACK_WEBHOOK_URL` secret is unset), **STOP AND ASK** — that's a secret-provisioning question for David.

## Done when

- All three workflows send a Slack alert on failure (verified by forcing a failure run and observing the message)
- Green runs do NOT alert (no noise)
- WORKLOG entry links the verification run URLs
- Commit sits locally, ready for pr-reviewer

## Context links

- Track K handover: `docs/handovers/Davv5-Track-K-*.md`
- slackapi/slack-github-action README: https://github.com/slackapi/slack-github-action (reference for input shapes)
- `.claude/rules/observability.md` if it exists, otherwise the Track K WORKLOG entry (2026-04-20)
