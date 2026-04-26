# Proposal - Vendor API Corpus Expansion (Generative Discovery Pass)

**Status:** Parked. Do not execute until trigger conditions below are met.

**Captured:** 2026-04-25, during the Coverage Matrix / Source Shapes drafting session.

**Owner:** David. Solo-operator engagement.

---

## What this is

Load the public API documentation for D-DEE's confirmed data sources into a NotebookLM corpus, then run a structured generative pass per vendor to surface candidate business questions and dashboards that are NOT in the current Business Question Map. Output flows back into the map as Q14+ candidates for triage.

This is not "look up shape on demand." That use case is light enough to handle with the URL stubs already in `docs/discovery/source-shapes.md`. This is "read the API surface end-to-end and find the dashboards we haven't thought of."

## Why parked

The current 13-question Business Question Map plus the Coverage Matrix and Source Shapes spec already define more buildable work than v2 will ship. Running an expansion pass before the existing 13 are proven dilutes focus and risks generating candidate questions that compete for attention against unfinished work.

The expansion pass is a v3 / "optimize further" move once the current map's value has been demonstrated by shipped Evidence.dev dashboards and a weekly digest firing.

## Trigger conditions

Start this proposal when ANY of the following is true:

- The first wave of dashboards built against the Coverage Matrix has shipped to D-DEE and is in active use for at least one full reporting cycle.
- David explicitly signals "what should we build next?" or "where's the next 10x?" - the natural cue for an expansion pass.
- A specific D-DEE stakeholder asks a question that the current 13-row map cannot answer, suggesting the map is undersized for the actual decisions being made.
- Phase B is winding down and a Phase C / v3 scope conversation begins.

Do NOT start this proposal in response to:

- A blocked cell on the Coverage Matrix (those are unblocking moves, not expansion moves).
- A new feature shipped by one of the vendors (file under `docs/_archive/` if interesting; do not let it pull this proposal forward).
- General "let's improve the docs" instinct without a shipped-dashboard milestone behind it.

## Recommended shape (when triggered)

### Notebook

- **One consolidated NotebookLM notebook**, engagement-scoped (lives in `.claude/corpus.yaml` under a new `engagement.vendor_apis` key or similar).
- **Engagement-scoped, not methodology-scoped.** D-DEE-specific config (location ID, custom fields, pipeline IDs) is entangled with the docs questions; cleaner to dispose of the whole notebook at engagement end than to extract portable parts.
- **Loaded sources:** GHL (highlevel.stoplight.io), Calendly (developer.calendly.com), Typeform (developer.typeform.com), Fathom (whatever public surface exists), Fanbasis (when D-DEE shares the docs - this is the bottleneck).

### Sources to skip

- **Stripe** - LLMs already know Stripe's API surface from training data; banned at D-DEE so re-querying is rare. Bookmark only.
- **Slack** - out of scope per the Q13 governance call. Reload only if Q13 flips.

### Order of operations when triggered

1. Confirm Fanbasis docs are available (or proceed without and add them later).
2. Edit `.claude/corpus.yaml` to add the new scope key.
3. Use `nlm` CLI to create the notebook and add each vendor's docs as sources. Order: GHL first (largest surface, biggest expansion pool), Calendly second (cleanest docs, fastest pass), Typeform third, Fathom fourth, Fanbasis last.
4. Update `.claude/rules/using-the-notebook.md` with the new scope value and when to use it.
5. Run the generative-pass query template (below) once per vendor.
6. Capture results in a new artifact: `docs/discovery/expansion-candidates.md`.
7. Triage the candidates with David - park / consider / promote to Q14+.
8. Promoted candidates go into `docs/discovery/business-area-map.md` as new rows.

### Generative-pass query template (per vendor)

> Given [vendor]'s API surface, identify endpoints, fields, events, or object relationships that could enable business questions about (a) attribution, (b) retention, (c) refund or churn risk, (d) operational health, or (e) entirely new dimensions we haven't yet considered. For each, name the data path and the candidate business question it would enable. Cite the specific endpoint or field URL. Surface at least five candidates per vendor; flag any that depend on data we already have but aren't yet using.

Run this verbatim per vendor via `ask-corpus` scoped to the new key.

### Expansion candidate doc shape

Each entry in `docs/discovery/expansion-candidates.md` should carry:

- **Source vendor** - which API doc reading surfaced this.
- **Data path** - the specific endpoint, field, event, or relationship that revealed the new attribution path.
- **Candidate question** - the business question it would enable, in the same plain-English voice as `business-area-map.md`.
- **Playbook chapter** - which canonical chapter (Funnel, Attribution, Conversion, Net Revenue, Retention, Conversation Intelligence, or a new chapter if needed).
- **Effort estimate** - rough size: small (already-landing data), medium (extractor or staging build), large (vendor-support-blocked).
- **Triage tag** - park / consider / promote.

## Concrete examples of what this could surface (seeded so the trigger pass has a starting point)

These are example candidates from the 2026-04-25 conversation, captured here as proof the framing works. The actual generative pass will produce more, grounded in the docs.

- **GHL non-booked-pipeline opportunities** - the entire opportunity universe outside the current booked-stage focus. What pipelines does D-DEE have that the matrix isn't measuring?
- **GHL tag taxonomy mining** - what tags exist on contacts that could be predictive segments?
- **GHL native forms** - separate from Typeform; if D-DEE uses any, they're a lead source the matrix doesn't list.
- **GHL workflow execution events** - which automations fire, which fail. Operational health beyond the SDR layer.
- **Calendly routing form responses** - pre-qualifying questions tied to bookings. Could enable "which pre-qual answers predict show rate?" - a brand-new question class.
- **Typeform payment blocks** - if any D-DEE form takes payment, that's a parallel revenue path next to Fanbasis.
- **Typeform logic jumps** - funnel-within-the-form. Which question kills response rate?
- **Fanbasis affiliate / referral attribution** if it exists - could create a new Referral chapter (the missing one in the AARRR map).
- **Fanbasis coupon, plan-change, failed-payment events** - retention signals beyond raw transactions.
- **Fathom AI summaries and action items** - often available even when full transcripts are gated. Could partially unblock Q12 without solving the transcript plan-tier problem.
- **Fathom CRM sync events** - does Fathom already push call outcomes into GHL? If yes, that's a join we don't know about.

## Open decisions to make at trigger time

These are not blockers today. They become live questions when the proposal is triggered:

1. **Notebook scope.** Confirm `engagement.vendor_apis` (engagement-scoped, disposable at engagement end) is still the right call vs methodology-scoped if D-DEE-style agency engagements look likely to repeat.
2. **Triage owner.** Decide whether triage of candidates happens with David alone or with a D-DEE stakeholder in the loop.
3. **Promotion gate.** What proves a candidate is worth promoting from `expansion-candidates.md` into `business-area-map.md`? The current map has D-DEE leadership / Precision Scaling / SDR-manager etc. as "Who cares" - new candidates need a named stakeholder before they earn a row.
4. **Fanbasis without docs.** If Fanbasis docs still haven't shipped at trigger time, decide whether to run the expansion pass on the other four and circle back, or wait for the full set.

## How this proposal closes out

When the expansion pass runs and the candidates are triaged:

- Promoted candidates land as new rows in `docs/discovery/business-area-map.md`.
- The Coverage Matrix and Source Shapes get updated to cover any new sources or fields the new questions surface.
- This proposal moves to `docs/_archive/` with a one-line note pointing at the resulting Business Question Map version.
- The vendor_apis notebook stays live in NotebookLM as an ongoing reference resource for the rest of the engagement.

---

## Reference

- Conversation that captured this proposal: 2026-04-25, during the Coverage Matrix and Source Shapes drafting pass. Not committed to chat logs in the repo; lives in agent session history.
- Related artifacts already shipped:
  - `docs/discovery/business-area-map.md` - the 13-question map this proposal would expand.
  - `docs/discovery/coverage-matrix.md` - the source-by-question coverage view.
  - `docs/discovery/source-shapes.md` - the field-level builder spec; the URL stubs in here are the seed for which docs to load when this proposal triggers.
- `.claude/rules/using-the-notebook.md` defines the corpus scope pattern this proposal extends.
