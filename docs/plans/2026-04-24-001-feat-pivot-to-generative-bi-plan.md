---
title: Pivot D-DEE BI from Metabase to dabi (Kim-style generative BI)
type: feat
status: superseded
parked_on: 2026-04-26
superseded_on: 2026-05-01
superseded_by: docs/plans/2026-05-01-001-feat-dashboard-product-plan.md
reactivation_gate: |
  Do not reactivate as the current BI direction. Superseded 2026-05-01 by
  the click-around dashboard product plan ("Cabinet shell, Kim simplicity").
  This file remains historical context only.
date: 2026-04-24
origin: conversation 2026-04-24 (this session) + docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md (the Port plan, paused) + Joshua Kim's Medium piece
supersedes: U5 of the Port plan ("Repoint Metabase at project-41542e21-..."); the "v2 deliverable: Evidence.dev cutover" line in CLAUDE.local.md; the `project_evidence_dev_transition.md` memory (since replaced by `project_bi_direction_dabi.md` on 2026-04-26)
related: docs/plans/2026-04-24-002-feat-generative-bi-platform-plan.md (Plan B — the dabi build); docs/plans/2026-04-24-strategic-reset.md (active Discovery Sprint); docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md (Port plan, paused at U3-complete)
---

# Pivot D-DEE BI from Metabase to dabi

> **SUPERSEDED 2026-05-01.**
> Current BI direction is the click-around dashboard product in
> `docs/plans/2026-05-01-001-feat-dashboard-product-plan.md`, not dabi.
> Use this file only for historical context.

## Fresh session startup

If you're opening this plan in a new Claude session, do these first:

1. Read `.claude/state/project-state.md` — confirm we are still in the Strategic Reset Sprint (docs-only) or have transitioned to the Gold-layer rebuild.
2. Read `docs/plans/2026-04-24-strategic-reset.md` — the active sprint plan. Its exit criteria gate every Plan A code unit.
3. Read `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — the paused Port plan. U5 is the unit Plan A pivots; U4a stays as the upstream parity gate.
4. Read `docs/plans/2026-04-24-002-feat-generative-bi-platform-plan.md` (Plan B) — the platform that ships the artifacts Plan A consumes.
5. Read this plan in full. **U1 is the only unit that is sprint-compatible and lands during the docs-only window.** Everything else waits for sprint exit + Port plan resume + Plan B Phase 1.

---

## Overview

The Port plan currently ships v2 by repointing Metabase v1.6 onto the consolidated GCP project (U5). `CLAUDE.local.md` says v2 is the Evidence.dev cutover. Both are now superseded.

The new direction: ship Speed-to-Lead on **dabi** — a Kim-style generative-BI system. Claude Code authors `dashboards/<name>/<file>.html`, the HTML calls a Cloud Run query proxy on the existing GCE VM, magic-link auth gates SDR/AE/Manager viewers. No Metabase, no Evidence framework runtime.

This plan is the engagement-level integration: it pivots the in-flight Port plan, defines the parity gate against Metabase v1.6, and decommissions Metabase post-cutover. **The platform itself is built in Plan B** (`docs/plans/2026-04-24-002-feat-generative-bi-platform-plan.md`); Plan A consumes its outputs.

**Sprint reality.** The Strategic Reset Sprint is active and explicitly bans warehouse / mart / dashboard PRs until the Gold-layer roadmap exists. Plan A's U1 is docs-only and lands during the sprint. U2 / U3 / U4 wait for the sprint to exit AND for Plan B Phase 1 to ship AND for Port plan U4a plumbing parity to hold.

---

## Problem Frame

Metabase v1.6 ships D-DEE's Speed-to-Lead today, but three frictions have crystallized:

1. **Iteration is slow.** Every dashboard change is a Python authoring script that calls the Metabase REST API. SDRs ask for a new column ("split by source"), and the round-trip is a PR cycle.
2. **No per-viewer slice.** The current public URL serves one shared view. SDRs cannot see "my leads" without provisioning Metabase accounts and re-doing dashboard permissions, which the deferred-work in CLAUDE.local.md flags as the next step.
3. **Public-or-Metabase-accounts dichotomy.** The current URL leaks all team metrics to anyone with the link. The only Metabase-native fix is per-user accounts on the OSS instance, which adds a license/admin surface neither party wants.

Evidence.dev (the previously-planned v2) solves #1 (Markdown+SQL is git-versioned and Claude-Code-editable) but does not solve #2 or #3 cleanly. **Joshua Kim's "generative BI" pattern** — Claude Code emits standalone HTML dashboards backed by live BigQuery — solves all three when adapted for non-technical viewers (Cloud Run query proxy + magic-link auth in front of Kim's HTML+SQL pages).

This plan moves D-DEE off Metabase to dabi for the v1 surface, which puts the engagement on a substrate that scales to the Gold-layer roadmap dashboards as they come online.

---

## Requirements Trace

- R1. Speed-to-Lead headline metric and v1.6 tile content reproduce identically on dabi (cite Port plan R3 for tolerance: ± 0 denominator, ± 1 numerator). Locked metric definition in `CLAUDE.local.md` is unchanged.
- R2. The public Metabase URL `34-66-7-243.nip.io/public/dashboard/...` (or its consolidated-project successor) continues to serve Speed-to-Lead through cutover; viewers see no broken-link window.
- R3. Post-cutover, every D-DEE viewer accesses Speed-to-Lead via magic-link login; no public-anyone-can-see-this URL remains live for production data.
- R4. Metabase OSS container, Postgres metadata DB, and Caddy reverse-proxy block are decommissioned within 30 days of cutover stability; `3-bi/metabase/` archived under `docs/_archive/`; `.claude/rules/metabase.md` retired.
- R5. `CLAUDE.local.md`'s "v2 deliverable" line, `.claude/state/project-state.md`, the Port plan's U5 + R2, and the `project_evidence_dev_transition.md` memory are updated to name dabi (not Evidence.dev / not Metabase repoint) before any other unit fires.

---

## Scope Boundaries

- This plan does **not** build the dabi platform. Plan B does. Plan A only consumes its outputs (the lib, the proxy, the Skill, the auth, the Caddy route, the runtime).
- This plan does **not** change Port plan U2 (dbt retarget — done), U3 (staging shims — done), U4a (plumbing parity — still gates Plan A U2), U6+ (extractor ports — orthogonal).
- This plan does **not** ship a second dashboard beyond Speed-to-Lead. New dashboards are scoped by the Gold-layer roadmap (sprint output) and built on dabi after Plan A cutover holds.
- This plan does **not** rewrite the locked Speed-to-Lead metric. Only the rendering layer changes.

### Deferred to Follow-Up Work

- **Per-SDR filtered Speed-to-Lead view.** Ship Speed-to-Lead with one shared view first; per-SDR filter is a fast-follow once magic-link is live and the roster is in dabi. If Plan B's auth implementation makes it a 30-min add, fold it into Plan A U3.
- **Weekly digest email firing from dabi.** Currently on the Port plan's deferred list; reroute to dabi after cutover stability.
- **Migrating other client engagements off Metabase.** D-DEE-only decision. The PS template stays on `3-bi/metabase/` until proven elsewhere.
- **The Gold-layer roadmap's other dashboards.** Each new business-area dashboard from the sprint roadmap lands as its own Plan A-style integration, consuming Plan B's platform.

---

## Context & Research

### Relevant Code and Patterns

- `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — the Port plan; U5 is the unit Plan A pivots in place
- `docs/plans/2026-04-24-strategic-reset.md` — active Discovery Sprint; gates Plan A's build units
- `3-bi/metabase/runtime/Caddyfile` — current routing config (`/` reverse-proxies to Metabase; Plan A U3 swaps it)
- `3-bi/metabase/runtime/docker-compose.yml` + `startup-script.sh` — the existing GCE VM stack; Plan A U4 destroys the metabase service block
- `3-bi/metabase/authoring/dashboards/speed_to_lead.py` — the v1.7 Python authoring script; the *queries* in it are the spec for what dabi U2 reproduces
- `2-dbt/models/marts/rollups/speed_to_lead/*` + `2-dbt/models/marts/speed_to_lead_detail.sql` — the dbt models dabi queries hit (per state file: 15/15 cards now aggregate directly on `speed_to_lead_detail`)
- `CLAUDE.local.md` — locked metric definition (parity baseline)

### Institutional Learnings

- `feedback_multi_agent_orchestration.md` — supersedes `feedback_ship_over_ceremony.md` (2026-04-27). Main-session execution remains default for solo build steps; spawn parallel/specialized agents (CE reviewers, Altimate skills, data-engineer agent) when fanning out independent work.
- `feedback_preserve_working_infra.md` — drives the "keep the Caddy / nip.io URL stable through cutover" decision.
- `project_evidence_dev_transition.md` — captured the WHY behind moving off Metabase (Claude-Code-editability of git-versioned source). dabi achieves the same goal without the Evidence framework. **Action:** this memory is updated/superseded in Plan A U1.
- `project_gcp_consolidation_decision.md` — confirms the consolidation target; Plan A's hosting decisions reuse `project-41542e21-470f-4589-96d` infra.

### External References

- [Joshua Kim, "Building Dashboards Without BI SaaS"](https://joshua-data.medium.com/generative-bi-en-3669ffd08ddb) — the source idea; Plan B captures the architecture in detail.
- Strategic Reset sprint plan (above) — gates Plan A's build units.

---

## Key Technical Decisions

- **Cutover, not parallel-run.** Once dabi's Speed-to-Lead matches Metabase v1.6 tile-by-tile (Plan A U2 hard gate), the URL flips. Metabase stays running on a `/legacy-metabase` sub-path for a 30-day rollback window, then decommissions in U4. *Rationale:* one user-facing surface at a time; rollback is a Caddy config revert.

- **Same hostname, route swap.** dabi reuses the existing `34-66-7-243.nip.io` (or its consolidated-project successor). Caddy routes `/` to the dabi static dashboard, `/api/query/*` to the proxy, `/auth/*` to the magic-link service. *Rationale:* zero viewer-facing URL change; bookmarks survive; no DNS work; reuses the cert + Caddy + ACME state already on the VM.

- **Speed-to-Lead is the only Plan A dashboard.** Other dashboards land separately, after Plan A cutover holds and the Gold-layer roadmap names them. *Rationale:* one parity surface to gate, not many; matches the sprint's "rebuild Gold against real priorities" posture.

- **Magic-link from day 1.** dabi ships authed at cutover. Do not run a public-then-add-auth sequence. *Rationale:* avoids a second cutover; matches the deferred follow-up in CLAUDE.local.md ("Retirement of the public dashboard URL once SDR/Manager accounts come online"); the agency owns the user list, no Google accounts needed for D-DEE staff.

- **Plan B blocks Plan A's build units.** Plan A U2 cannot start until Plan B Phase 1 ships (lib + proxy + Skill + first sample). Plan A U3 cannot start until Plan B Phase 2 ships (magic-link auth + Caddy route integration). *Rationale:* parity gate needs the platform to exist; cutover gate needs auth to exist.

- **Sprint exit blocks Plan A's build units.** U2 / U3 / U4 are warehouse / mart / dashboard PRs in the sprint's "out of scope" sense and explicitly cannot land during the sprint. U1 is docs-only and lands now. *Rationale:* sprint ban on build PRs is the real-current-state constraint, not a Plan A choice.

- **Plan A U1 lands the decision visibly across four files.** State file, CLAUDE.local.md, the Port plan U5 (in place, no renumbering), the `project_evidence_dev_transition.md` memory, plus a WORKLOG entry. *Rationale:* the decision must propagate to every artifact a future Claude session auto-loads, otherwise drift surfaces immediately on session resume.

---

## Open Questions

### Resolved During Planning

- *Does dabi cutover happen before or after Port plan U4a plumbing parity?* After. dabi reads from the same dbt models that U4a proves are correct; Plan A U2 inherits U4a's gate.
- *Does the public dashboard URL change?* No — same hostname, Caddy route swap. The Metabase embed token does not transfer (dabi has no equivalent); magic-link auth replaces it.
- *Does this plan change the headline metric?* No. The metric definition in CLAUDE.local.md is preserved verbatim. Only the rendering layer changes.
- *Does Plan A interact with the Gold-layer roadmap?* Yes — Plan A consumes Speed-to-Lead from the existing dbt mart. The Gold-layer roadmap may add new marts that need dashboards; each future dashboard rides Plan B's platform via its own Plan-A-style integration.
- *Can Plan A U1 land during the Strategic Reset Sprint?* Yes. U1 is docs-only and updates only state, plan, memory, and CLAUDE.local.md files — no warehouse / mart / dashboard PRs.

### Deferred to Implementation

- *Whether the parity test for Speed-to-Lead is screenshot-diff or query-result-equality.* Decide in U2; query-result is more durable (and is what the Port plan's U4a uses for its own parity), screenshot is more user-facing. Default: both.
- *Whether per-SDR filter ships in Plan A's first dabi version or as fast-follow.* Defaults to fast-follow per scope; if Plan B's auth implementation makes the per-user `WHERE assignee = :user_id` injection a small add, do it in Plan A U3 instead.
- *Whether U4 archives `3-bi/metabase/` to `docs/_archive/` or deletes outright.* Default: archive. Decide in U4 based on terraform + git history retention needs.

---

## Implementation Units

- [ ] U1. **Land the pivot decision across state, plan, memory, CLAUDE.local (sprint-compatible, docs-only)**

**Goal:** Make the pivot decision visible and load-bearing in every artifact a future Claude session auto-loads. Without this, future Claude opens the Port plan, sees U5 = "Repoint Metabase," and does the wrong thing.

**Requirements:** R5

**Dependencies:** None. Lands during the Strategic Reset Sprint.

**Files:**
- Modify: `CLAUDE.local.md` ("v2 deliverable" line — change Evidence.dev → dabi; cite Plan A and Plan B by path)
- Modify: `.claude/state/project-state.md` (Last 3 decisions — add the pivot; Active plan section — add Plan A and Plan B as related; Open threads — add "dabi build queued for post-sprint")
- Modify: `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` (U5 — add a "**Superseded 2026-04-24:** see `docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md` U2/U3" note inside the unit body; R2 — annotate that the Metabase reference now means dabi served via the same Caddy host; do NOT renumber units)
- Modify: `WORKLOG.md` (today's entry: dabi pivot decision, why, how it interacts with the sprint)
- Modify or supersede: `~/.claude/projects/-Users-david-Documents-data-ops/memory/project_evidence_dev_transition.md` (Evidence.dev cutover is no longer the v2 plan; replace with the dabi rationale + link to Plan A and Plan B; or supersede with a new memory `project_dabi_transition.md` and mark the old one stale)

**Approach:**
- Read each file before editing. Targeted edits only — do not rewrite anything that is still true (e.g., the locked metric table in CLAUDE.local.md is unchanged).
- For the Port plan, append a single italicized "Superseded 2026-04-24" note at the top of U5's body. Leave the unit number and original body intact for history. Add the same note to R2 in the Requirements Trace section.
- For the memory file, prefer in-place update over supersede — keeps the memory key stable and avoids stale `MEMORY.md` index drift. Confirm the file path with `ls ~/.claude/projects/-Users-david-Documents-data-ops/memory/` before editing.

**Test scenarios:**
- *Happy path:* a fresh Claude session that auto-loads project-state + CLAUDE.local + memories gets the dabi pivot decision in its initial context, and would not propose "repoint Metabase" if asked about U5.
- *Verification:* `grep -n dabi CLAUDE.local.md` returns the v2-deliverable line; `grep -n "Superseded 2026-04-24" docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` returns the U5 + R2 hits; `grep -n dabi .claude/state/project-state.md` returns the Last-3-decisions entry.
- *Sprint check:* `git diff` after the unit lands touches only docs / state / plan / memory / WORKLOG files. No `2-dbt/`, no `3-bi/`, no `1-raw-landing/` files modified.

**Verification:**
- All five files updated; one WORKLOG entry; one memory updated or superseded.
- Sprint daily WORKLOG entry references the pivot decision.

---

- [ ] U2. **Author Speed-to-Lead in dabi; tile-by-tile parity vs Metabase v1.6 (HARD GATE)**

**Goal:** Reproduce every Metabase v1.6 Speed-to-Lead tile inside dabi. Metric values match the locked-metric definition exactly. Until parity holds, the cutover (U3) does not run.

**Requirements:** R1

**Dependencies:**
- U1 (decision visible to future sessions)
- Strategic Reset Sprint exit (per `docs/plans/2026-04-24-strategic-reset.md` exit criteria; the Gold-layer roadmap published at `docs/plans/2026-05-xx-gold-layer-rebuild.md`)
- Plan B Phase 1 shipped (lib + proxy + Skill + first sample dashboard)
- Port plan U4a plumbing parity green

**Files:**
- Create: `3-bi/dabi/dashboards/speed_to_lead/index.html` (authored by the dabi-author Skill)
- Create: `3-bi/dabi/dashboards/speed_to_lead/queries.sql` (per-tile SQL files; structure follows Plan B U1's convention)
- Create: `docs/parity/dabi-speed-to-lead-parity.sql`
- Create: `docs/parity/dabi-speed-to-lead-screenshots/` (before/after tile screenshots for sign-off)
- Create: `2-dbt/tests/dabi_speed_to_lead_metric_parity.sql` (compares the dabi-served headline value vs the Metabase v1.6 baseline; uses BigQuery time-travel on the Metabase mart side if the cutover window has shifted state)

**Approach:**
- For each Metabase v1.6 tile (T1 hero, T2/T3 supporting chips, T6 % with 1-hour activity, response-time distribution, close-rate-by-touch, source-performance, coverage heatmap, SDR leaderboard, lead-tracking match-rate donut), name the query and transcribe the SQL from the v1.7 `speed_to_lead.py` (which now reads directly from `marts.speed_to_lead_detail`).
- Register each query through Plan B's Skill so it lands in `3-bi/dabi/proxy/queries/<name>.sql` AND in `3-bi/dabi/dashboards/speed_to_lead/queries.sql`. The proxy's declared-query allowlist gates execution.
- Run the dabi page locally against the Cloud Run proxy pointed at `project-41542e21-...`.
- Capture before/after screenshots of every tile. Headline metric MUST match to the unit (per Port plan R3); bar/line tiles MUST match in shape, ordering, and labels.
- If anything diverges, root-cause to: (a) query bug — fix in dabi queries.sql, (b) dbt model drift — out of Plan A scope, escalate, (c) chart rendering quirk — fix in dabi-core or accept with a documented stylistic note.
- **Do not flip the cutover until every in-scope tile parity holds.**

**Execution note:** Test-first on the parity SQL. Write `docs/parity/dabi-speed-to-lead-parity.sql` and the dbt parity test BEFORE authoring the dabi page, so the gate is observable from the first Skill invocation.

**Patterns to follow:**
- `docs/parity/cutover-speed-to-lead-plumbing-parity.sql` (created in Port plan U4a) for parity-SQL shape
- `dbt_expectations.expect_table_row_count_to_equal_other_table` idiom for the dbt parity test

**Test scenarios:**
- *Parity (HARD GATE):* dabi-served headline metric value === Metabase v1.6 headline metric value, ± 0 denominator, ± 1 numerator (per Port plan R3 tolerance).
- *Parity:* every tile's underlying SQL returns the same row count and same key aggregates against `marts.speed_to_lead_detail`.
- *Visual:* before/after screenshots of every v1.6 tile show the same numbers (allow stylistic differences in chart appearance — color, font, padding — but not in the numbers or in the dimension/measure choice).
- *Negative:* if a tile depends on a Metabase feature dabi doesn't have (e.g., drill-through to a saved question), document it explicitly and route to Plan B fast-follow rather than papering over the gap.
- *Regression:* after the parity SQL passes, run it again 24 hours later; same result (rules out time-of-day query bugs).

**Verification:**
- All v1.6 tiles reproduced in dabi.
- Parity SQL returns zero rows (no divergence).
- David signs off on the screenshots before U3.

---

- [ ] U3. **Cutover: repoint public URL from Metabase to dabi; activate magic-link**

**Goal:** Flip the Caddy `/` route from Metabase to dabi static. Magic-link auth gates dashboard access. Viewers see the same hostname, log in once, see the same data.

**Requirements:** R2, R3

**Dependencies:** U2 parity green; Plan B Phase 2 shipped (magic-link auth + Caddy route integration); roster pre-loaded into dabi (David, the SDRs/AEs/managers from `2-dbt/seeds/ghl_sdr_roster.csv`, the agency PoCs).

**Files:**
- Modify: `3-bi/metabase/runtime/Caddyfile` (root site block — `/` reverse-proxies to dabi static; `/api/query/*` to the proxy; `/auth/*` to magic-link; the existing Metabase block moves to `/legacy-metabase` with `handle_path` so the rollback option stays alive)
- Create: `3-bi/dabi/authoring/users.yaml` (initial roster — emails + display names + role; populated from the SDR seed + agency contacts)
- Create: `docs/runbooks/dabi-cutover.md` (off-hours window, step-by-step, rollback, comms script for D-DEE)
- Modify: `WORKLOG.md` (cutover entry)
- Modify: `.claude/state/project-state.md` (Where we are — dabi serving Speed-to-Lead at the public URL)

**Approach:**
- **Roster first.** Send the magic-link signup emails one day before the cutover. Confirm at least one SDR + David can log in successfully BEFORE the Caddy swap.
- **Off-hours window.** Metabase load is ~zero outside business hours per typical D-DEE patterns. Verify with David. Default window: late evening Pacific, weekday.
- **The actual swap is a single git commit** that reorders the Caddyfile blocks. Push to the ops bucket per `3-bi/metabase/README.md`'s deploy flow; reset the VM. Two minutes of downtime expected.
- **Rollback.** `git revert` the Caddy commit; re-upload to ops bucket; reset the VM. Same path as any Caddy change. Practiced in pre-cutover dry run.
- **Comms.** Loom walkthrough of the new login flow sent to D-DEE 24 hours pre-cutover; magic-link copy in the email + on the login page is plain-language ("D-DEE, click the button below to log in to your dashboard"); office hours offered for the first day.

**Test scenarios:**
- *Happy path:* every roster user can request a magic link, click it, land on the Speed-to-Lead dashboard, and see the same numbers they saw on Metabase v1.6.
- *Edge case:* a viewer with an expired session gets routed to the magic-link login page, not a 500.
- *Edge case:* a viewer who lost the magic-link email can request a new one without an admin in the loop.
- *Rollback:* a single `git revert` on the Caddy commit + VM reset restores Metabase as the `/` route within 5 minutes.
- *Negative:* an unsolicited email address that requests a magic link does NOT receive one (roster-gated).

**Verification:**
- Public URL serves dabi (login wall first; same Speed-to-Lead numbers post-login).
- Metabase still reachable at `/legacy-metabase` for 30 days (rollback path live).
- `docs/runbooks/dabi-cutover.md` documents both directions and was followed step-for-step.
- D-DEE confirms successful login from at least three SDR + manager accounts within 24 hours.

---

- [ ] U4. **Decommission Metabase; extend Port plan U14 with the teardown bullets**

**Goal:** Tear down the Metabase OSS container, the Postgres metadata DB, the `/legacy-metabase` Caddy block. Archive `3-bi/metabase/`. Retire `.claude/rules/metabase.md`. Bundle the GCP-resource teardown into Port plan U14 so the cleanup rides the same 30-day soak window.

**Requirements:** R4

**Dependencies:** U3 cutover stable for 30 days (no rollback executed; no D-DEE viewer hitting `/legacy-metabase` per Caddy access logs); Port plan U14 ready to fire (which itself depends on Port plan U4b live-raw parity holding for 7 days).

**Files:**
- Modify: `3-bi/metabase/runtime/docker-compose.yml` (remove the `metabase` service block; keep `cloud-sql-proxy` only if dabi proxy needs it — verify in Plan B U7)
- Modify: `3-bi/metabase/runtime/Caddyfile` (delete the `/legacy-metabase` reverse-proxy block)
- Modify: `3-bi/metabase/terraform/*.tf` (destroy Metabase-specific resources: the Cloud SQL Postgres instance, the BQ reader SA + key Secret, the ops-bucket entries for `docker-compose.yml` and `Caddyfile` if they get split out for dabi; KEEP: the GCE VM, the static IP, the BQ data-reader SA if dabi reuses it)
- Move: `3-bi/metabase/` → `docs/_archive/2026-05-3-bi-metabase/` (preserve for reference; the runtime stopped serving 30 days earlier)
- Modify: `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` U14 — append Metabase-specific teardown bullets to the existing decom unit (Cloud SQL Postgres destroy, BQ reader key revocation, terraform state cleanup, ops-bucket cleanup)
- Create: `docs/runbooks/metabase-decommission.md` (final-backup procedure, terraform destroy order, post-destroy verification, where the archive lives)
- Modify: `CLAUDE.md` ("Current State" section — replace `3-bi/metabase/` with `3-bi/dabi/`)
- Delete: `.claude/rules/metabase.md` (the rule no longer applies; no `3-bi/metabase/**` paths exist)

**Approach:**
- **Final Postgres backup before destroy.** Cloud SQL automated backup → export the latest snapshot to GCS cold-storage bucket; retain 90 days. The dashboard authoring history is in the Postgres DB; the only source of truth for "what did Metabase look like at v1.6" lives there. Worth the cold-storage cost.
- **7-day Caddy access-log review.** Before destroy, grep the Caddy access logs for any hit on `/legacy-metabase`. Zero hits across 7 days = no forgotten consumer. Any non-zero count → investigate before destroy.
- **Terraform destroy is the only irreversible step.** Everything else is git-revertable. Stage the destroy with `terraform plan` review by David; execute on his explicit go-ahead.
- **U14 bundling.** The Port plan's U14 decommissions `dee-data-ops*` projects. Plan A U4 adds the Metabase-specific resources to U14's destroy list — same calendar window, same David-sign-off, one terraform apply rather than two.

**Test scenarios:**
- *Happy path:* `gcloud compute ssh metabase --command "docker ps"` shows no metabase container; dabi at `/` still works; `/legacy-metabase` returns 404.
- *Verification:* `terraform plan` after destroy shows zero diff for Metabase-specific resources.
- *Verification:* `ls 3-bi/` shows only `dabi/` (no `metabase/`).
- *Verification:* GCS cold-storage bucket contains the final Postgres backup, dated within 24 hours of destroy.
- *Negative:* a viewer hitting `/legacy-metabase` post-decom gets a clean 404, not a 502 (proxy block fully removed, not just unreferenced).

**Verification:**
- Metabase container destroyed; Cloud SQL Postgres destroyed (final backup in GCS); terraform state clean.
- `3-bi/metabase/` archived; `.claude/rules/metabase.md` deleted.
- `CLAUDE.md` updated; Port plan U14 includes Metabase teardown bullets.
- One BI surface remains: dabi.

---

## System-Wide Impact

- **Interaction graph:** Caddy `/` routing changes (U3) on the existing GCE VM. Terraform destroys (U4) touch shared GCE resources but exclude the VM, static IP, and BQ data-reader SA. dbt models are unchanged across all four units.
- **Error propagation:** A parity failure at U2 stops the cutover with no rollback needed (Metabase v1.6 still serving). A cutover failure at U3 is git-revertable on the Caddy config within 5 minutes. A destroy failure at U4 is recoverable from the GCS-cold-storage Postgres backup.
- **State lifecycle risks:** Metabase Postgres DB destroy in U4 is irreversible after the 30-day soak; the GCS cold-storage backup is the only safety net. dabi state (user roster in `users.yaml`, magic-link sessions in the proxy DB) is built fresh in U3 and is not impacted by Metabase destroy.
- **API surface parity:** Public URL hostname preserved. Embed token does not transfer (dabi has no equivalent). Any external consumer that hardcoded `https://34-66-7-243.nip.io/public/dashboard/<uuid>` loses access — but per CLAUDE.local.md the embed URL is not documented as embedded externally; verify in U3 by grepping any external D-DEE-facing artifacts (Loom-doc, internal SOP) for the URL.
- **Integration coverage:** U2 parity gate (dabi vs Metabase v1.6) is the single cross-layer test that proves the locked metric is preserved through the rendering swap. Unit tests on the dabi lib alone cannot prove it.
- **Unchanged invariants:** Speed-to-Lead headline-metric formula; SDR-attributed denominator definition; `marts.speed_to_lead_detail` schema and contents; every Port plan unit other than U5; the GCE VM + static IP + Caddy host + BQ data-reader SA.

---

## Risks & Dependencies

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Plan B Phase 1 slips, blocking U2 | Medium | Plan-blocking | Plan B is on the critical path; if it slips past the sprint exit, Plan A U2 starts late but Plan A U1 still lands. The Port plan's U5 falls back to "repoint Metabase" as the no-pivot baseline if the slip is fatal. |
| dabi tile parity (U2) misses an edge case (Metabase drill-through, native filter behavior) | Medium | Recoverable | Documented gracefully in U2; routed to Plan B fast-follow; cutover does not proceed until parity holds for the in-scope tiles. |
| Magic-link auth UX confuses an SDR on first login | Medium | Low — UX-only | Pre-cutover dry run with one SDR; clear copy in the magic-link email; Loom walkthrough sent before cutover; office hours on cutover day. |
| 30-day Metabase rollback window passes without incident, then a forgotten external consumer surfaces | Low | Recoverable | `/legacy-metabase` Caddy access logs monitored for 7 days pre-decom; Postgres backup retained in GCS for 90 days post-destroy; recovery requires VM-side restore (~1 hour). |
| Terraform destroy in U4 takes out a resource shared with dabi | Medium | Recoverable | Pre-destroy `terraform plan` reviewed; shared resources (VM, static IP, BQ data-reader SA, ops bucket) explicitly excluded from the destroy targets; Plan B U8 documents which resources dabi reuses. |
| Sprint exit slips past 2026-05-08, delaying Plan A U2/U3/U4 | Medium | Schedule slip only | U1 still lands. Plan B Phase 1 can also build during the sprint slip if the Sprint owner (David) explicitly carves out a Plan B docs-only sub-track. |
| The Gold-layer roadmap (sprint output) reprioritizes Speed-to-Lead below other dashboards | Low | Schedule shift | Plan A still ships Speed-to-Lead as the dabi proof-point even if the roadmap deprioritizes it; the dashboard exists today and viewers depend on it. The roadmap reorders new dashboards, not the existing one. |
| Pre-existing public URL hardcoded in a D-DEE-internal SOP, Loom, or Calendly confirmation email | Medium | UX disruption | U3 grep external D-DEE artifacts pre-cutover; if a hardcoded URL exists, the same hostname is preserved (only the path may differ); update the artifact pre-cutover or accept a redirect from `/public/dashboard/<old-uuid>` to `/` in the new Caddy config. |

---

## Phased Delivery

### Phase 1 — Decision (Day 0, sprint-compatible)
- U1 — land the pivot decision across state, plan, memory, CLAUDE.local

**Exit:** future Claude sessions auto-load the dabi pivot decision; sprint daily WORKLOG references the pivot.

### Phase 2 — Build & parity (Week of sprint exit, parallel with Plan B Phase 1)
- Plan B Phase 1 ships → unblocks Plan A U2
- Port plan U4a plumbing parity green → unblocks Plan A U2
- U2 — dabi Speed-to-Lead authored + tile-by-tile parity (HARD GATE)

**Exit:** parity SQL green; before/after screenshots signed off.

### Phase 3 — Cutover (Week after Phase 2, depends on Plan B Phase 2)
- Plan B Phase 2 ships → unblocks Plan A U3
- Roster pre-load + dry-run login + comms ship → cutover ready
- U3 — Caddy route swap; Metabase moves to `/legacy-metabase`

**Exit:** dabi serves Speed-to-Lead at the public URL; magic-link gates viewers; rollback path live.

### Phase 4 — Decommission (Day 30+ after cutover)
- U4 — Metabase teardown, archive `3-bi/metabase/`, bundle terraform destroy into Port plan U14

**Exit:** one BI surface (dabi); Metabase archived; Port plan U14 fires with Metabase teardown bundled in.

---

## Documentation / Operational Notes

- WORKLOG.md entry per phase boundary; daily one-liner during the sprint pause referencing this plan's status.
- `.claude/state/project-state.md` updated at U1 (decision visible) and U3 (cutover complete).
- `docs/runbooks/dabi-cutover.md` ships in U3; includes the rollback procedure step-for-step.
- `docs/runbooks/metabase-decommission.md` ships in U4; includes the final-backup procedure and the terraform destroy order.
- `.claude/rules/metabase.md` retires in U4 (deleted alongside the directory move; future paths are `3-bi/dabi/**`, governed by `.claude/rules/dabi.md` from Plan B U1).
- The dabi cutover is a candidate for a public WORKLOG-style write-up (PS marketing) — Joshua-Kim-style adapted to a real engagement. Decide post-U3.

---

## Sources & References

- **Origin:** conversation 2026-04-24 (this session); [Joshua Kim, "Building Dashboards Without BI SaaS" — Medium, April 2026](https://joshua-data.medium.com/generative-bi-en-3669ffd08ddb)
- **Port plan (paused, partly superseded):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`
- **Strategic Reset Sprint (active):** `docs/plans/2026-04-24-strategic-reset.md`
- **Plan B (the dabi platform build):** `docs/plans/2026-04-24-002-feat-generative-bi-platform-plan.md`
- **Locked metric:** `CLAUDE.local.md` Speed-to-Lead table
- **Memory updated in U1:** `~/.claude/projects/-Users-david-Documents-data-ops/memory/project_evidence_dev_transition.md`
- **Existing BI surface (cutover source):** `3-bi/metabase/`
