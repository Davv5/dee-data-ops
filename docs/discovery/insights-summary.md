# Discovery Sprint — Insights Summary

**Sprint supporting artifact.** Executive one-pager across the discovery packet. Written for: David, future Claude sessions, and D-DEE stakeholders reading a condensed brief.

Last regenerated: 2026-04-24.

---

## What the sprint is

A 1–2 week Phase A (Steering) pause on new build, running 2026-04-24 → ~2026-05-08. Goal: produce four discovery artifacts so Phase B (Velocity) resumes on a Gold-layer roadmap that matches actual data reality — not assumed reality. See `docs/methodology.md` for the phase model and `docs/plans/2026-04-24-strategic-reset.md` for the sprint plan.

---

## The five highest-leverage findings

1. **Fanbasis is D-DEE's live revenue source and has zero staging models.** Raw data lands fresh daily; the dbt side has never been wired. This is the single biggest unlock available today — ~1–2 hours of pure dbt work, survives any roadmap churn, opens every revenue-area business question. Per memory, Stripe is banned (historical-only), so this is *the* payments path.

2. **GHL data is 3–4 days stale because `bq-ingest` is broken.** Richest business-signal source in the stack (contacts, opportunities, conversations, pipelines, SDR activity) currently delivers a stale, partial view. The fix lives in the GTM repo, not Merge — outside sprint scope to repair, but surfacing it in the Gold-layer roadmap preconditions is Phase A work.

3. **GHL has two extraction paths and both are compromised.** Phase-2 per-object path: 4 entities empty (`messages`, `notes`, `tasks`, `users`), `conversations` 92% undercounted (101 vs 1,314). Legacy blob path: broader coverage but also stale. **The biggest decision pending in this sprint is which path becomes authoritative for the Gold rebuild.** That decision alone resolves five sub-issues.

4. **Fathom captures call metadata but 0% transcripts.** 1,157 calls landed with no transcript payload. The LLM-analysis pipeline that would classify them has been dead since 2026-04-03. Metadata-only questions work today; content questions are blocked. The sprint should *decide whether the Gold roadmap needs transcripts* — that sizes the fix.

5. **The staging layer holds exactly 13 models and every one of them has at least one open caveat.** Zero fully-healthy models. This isn't a crisis — it's the discovery: the foundation is intact, but the current state was opaque to the team. Making it visible is the sprint's core value.

---

## Data reality snapshot

| | Count |
|---|---:|
| Sources declared | 7 |
| Sources actually delivering data to BQ | 5 (Calendly, GHL, Fathom, Typeform, Fanbasis) |
| Sources with staging models | 5 (everything except Fanbasis) |
| Staging models on disk | 13 |
| Staging models fully healthy (fresh, populated, no caveats) | **0** |
| Staging models compiling empty (0 upstream rows) | 2 (`stg_ghl__messages`, `stg_ghl__users`) |
| Whole sources unmodelled despite live raw | 1 (Fanbasis) |
| Ambiguous sources (listed but may not be a source) | 1 (Slack) |

---

## Strategic takeaways

- **Foundation is sound.** BigQuery + dbt + 13 staging + raw-landing discipline all work. The Strategic Reset is not a rebuild; it's a re-scope. Every finding here is addressable through staging + roadmap decisions, not architectural rework.
- **The real problem was visibility, not tech.** v1 Speed-to-Lead shipped in 4 days (PRs #40–#48) without this inventory existing. It worked because the metric was narrow. Broadening scope without this visibility would multiply rework.
- **The Fanbasis gap is the sprint's #1 quick win.** All other P0 items (GHL path decision, `bq-ingest` repair) involve either deferred fixes or documentation; Fanbasis is the one we can actually *ship staging on* during Phase A and still be reversible.
- **"Research preview" labeling is the mechanism for moving without locking.** Per methodology Part 2 — anything we produce during the sprint that might change with the Gold-layer roadmap ships as preview, not contract.

---

## What this sprint will and will not produce

### Discovery packet

- `docs/discovery/source-inventory.md` ✅ already landed (source-centric view)
- `docs/discovery/staging-models.md` ✅ (this sprint, landed 2026-04-24)
- `docs/discovery/gap-analysis.md` ✅ (this sprint, landed 2026-04-24)
- `docs/discovery/insights-summary.md` ✅ (this file)
- `docs/discovery/business-area-map.md` — pending. Enumerates D-DEE business areas, stakeholders, key questions per area, data deps.
- `docs/discovery/coverage-matrix.md` — pending. Grid: areas × sources with current/target/gap per cell.
- `docs/discovery/gold-layer-roadmap.md` — pending. Prioritized marts, each with preview/contract classification and Phase B unlock criteria.

### Will NOT produce

- `bq-ingest` repair (GTM repo).
- Fathom transcript landing fix (U6 post-sprint).
- Typeform `form_id` extractor change (U9 post-sprint).
- Any net-new mart. Phase A rule: no mart growth until Gold-layer roadmap locks.
- Stripe backfill repair (moot; account banned).

---

## Phase-transition criteria

Phase B (Velocity) reactivates when:

1. `docs/discovery/gold-layer-roadmap.md` exists with prioritized marts, each classified as preview or contract, each with unlock criteria.
2. David signs off on the roadmap (sole-operator = self-approval, logged in WORKLOG.md).
3. The GHL Phase-2-vs-legacy decision is documented.
4. The Fanbasis scoping decision (which entities to prioritize in Gold) is documented.

Transition is explicit, logged, and irreversible-without-another-Reset. See `docs/methodology.md` § "The phase gate".

---

## Cross-reference

- Source-by-source: `docs/discovery/source-inventory.md`
- Model-by-model: `docs/discovery/staging-models.md`
- Gap-by-gap with priority and phase classification: `docs/discovery/gap-analysis.md`
- Methodology + phase model: `docs/methodology.md`
- Sprint plan: `docs/plans/2026-04-24-strategic-reset.md`
