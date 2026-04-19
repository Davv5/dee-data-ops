# Client v1 Scope — Speed-to-Lead Dashboard

**Status:** Draft for client alignment
**Author:** David
**Date:** 2026-04-19
**Target ship:** 2–3 weeks from kickoff

---

## 1. Executive summary

High-ticket coaching client runs a book-a-call funnel. Lead volume is small (<50 sales-ready applications/day), but the team operates under layered response-time SLAs that directly drive show rate and close rate. There is no warehouse today — data is scattered across GoHighLevel, Typeform, Calendly, Stripe, Fanbasis, Fathom, and Slack.

**v1 delivers:** a BigQuery-backed dbt project with a Looker Studio dashboard that measures the one metric that matters — **% of booked calls confirmed by an SDR within 5 minutes** — plus the supporting volume, rep-level, and funnel-attribution cuts that make the headline number actionable.

**v1 explicitly does not deliver:** revenue/LTV modeling, real-time alerting, Slack ingestion, call-outcome analytics. Those are v1.5 and v2 — architected for but not built.

---

## 2. Business context

- **Offer:** high-ticket coaching / info-product
- **Funnel shape:** lead magnet (Typeform) → optional book-a-call (Calendly) → SDR triage call → AE closing call → Stripe or Fanbasis checkout
- **Team:** 6+ SDRs ("appointment center") + 4+ AEs
- **Lead assignment:** manual claim-the-lead via Slack emoji reaction; first SDR to dial through GHL effectively owns the lead
- **Activity logging:** mostly manual outside GHL's native dialer / SMS — this is a known data-quality gap and the dashboard intentionally exposes it

---

## 3. What "speed-to-lead" actually means here

This is not classic inbound SaaS speed-to-lead. The team runs a book-a-call motion with layered SLAs:

| Moment | Start clock | Stop clock | SLA |
|---|---|---|---|
| **Primary — Confirmation** | Call booked in Calendly | First outbound SDR call in GHL | 5 min |
| **Secondary — No-show rescue** | Scheduled call start passes without a show | First outbound SDR call post-miss | TBD with client |
| **Tertiary — Unbooked form chase** | Typeform fill without a booked call | First outbound SDR touch | TBD with client |

**v1 ships the primary.** Secondary and tertiary are architected for (same staging + warehouse models power all three) but only the primary metric is exposed in the dashboard at launch. Confirmation of the two follow-up SLA thresholds is a kickoff-week task.

---

## 4. Data sources & ingestion architecture

Six sources, three ingestion patterns, one repo.

| Source | Role | Ingestion | Cost |
|---|---|---|---|
| Typeform | Lead-magnet submissions, funnel attribution | Fivetran native | Free tier |
| Calendly | Booked calls — **start clock** | Fivetran native | Free tier |
| Stripe | Primary revenue | Fivetran native | Free tier |
| GoHighLevel | Calls/SMS, pipeline stages, SDR attribution (by personal number) | Custom Python → BigQuery, run via GitHub Actions cron | Free |
| Fanbasis | Secondary revenue (payment plans) | Custom Python → BigQuery, run via GitHub Actions cron | Free |
| Fathom | Meeting held / show rate / outcomes — **v1.5** | Custom Python → BigQuery | Free |
| Slack | Emoji-claim signal — **v2** | Deferred | — |

**Rationale for GitHub Actions cron + Python** over Airbyte/Hevo: $0 ongoing cost, no VM to babysit, templates cleanly for client #2, and the same repo holds both ingestion and dbt code — one source of truth per client.

**Fivetran free-tier sizing:** <50 applications/day + small Typeform/Calendly/Stripe volumes stay under the 500K MAR cap by a wide margin. No risk of overage.

---

## 5. v1 dashboard contents

Looker Studio (free, BigQuery-native) — one page, five tiles:

1. **Headline:** `% of booked calls confirmed within 5 minutes (logged)` — last 7 days, with WoW delta
2. **Median confirmation time** — in minutes, last 7 days, by SDR
3. **Volume:** booked calls per day, stacked by lead-magnet / funnel source
4. **Data-quality diagnostic:** `% of booked calls with any SDR activity logged in GHL within 1 hour` — exposes the activity-logging gap as a management-visible metric
5. **Rep leaderboard:** SDRs ranked by (a) volume claimed and (b) % within SLA, last 30 days

**Explicit framing in the dashboard:** all metrics are computed from *logged* activity in GHL. Reps who use the GHL dialer appear; reps who dial from their cell phone off-platform do not. This is the forcing function to push everyone onto the GHL dialer.

---

## 6. Repo & modeling structure

Single repo, client-specific fork of the `dbt-dataops-template` pattern:

```
client-name/
├── ingestion/
│   ├── ghl/              # Python pulls for GHL API → BQ raw
│   ├── fanbasis/         # Python pulls for Fanbasis API → BQ raw
│   └── fathom/           # (v1.5) Python pulls for Fathom
├── .github/workflows/
│   ├── ingest.yml        # Cron: daily GHL + Fanbasis pulls
│   ├── ci.yml            # dbt build on PR against CI schema
│   └── deploy.yml        # dbt build against prod post-merge
├── dbt/
│   ├── models/
│   │   ├── staging/      # stg_<source>__<table> — views, 1:1 with sources
│   │   ├── warehouse/
│   │   │   ├── dimensions/    # dim_contacts, dim_sdrs, dim_aes, dim_offers
│   │   │   └── facts/         # fct_calls_booked, fct_sdr_outreach, fct_revenue
│   │   └── marts/             # sales_activity_detail (wide, denormalized — powers all v1 + v1.5 dashboards)
│   ├── macros/generate_custom_schema.sql
│   └── dbt_project.yml
└── .claude/
    ├── rules/            # Auto-loading conventions per folder
    └── skills/           # Reusable workflows
```

Three-environment design: `dev_david`, `ci`, `prod` — with `prod` split into `staging` / `warehouse` / `marts` schemas via the `generate_schema_name` macro.

**Naming convention across layers:** `staging` and `warehouse` keep Kimball technical naming (`stg_`, `dim_`, `fct_`) so internal contributors can reason about grain and role at a glance. The `marts` layer deliberately **drops** `fct_`/`dim_` prefixes in favor of business-friendly names (`sales_activity_detail`, not `fct_sales_activity`) because the mart schema is the surface the client and BI tools touch, and fact/dim language isn't helpful to non-data stakeholders (source: *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook).

**Fewer, wider marts over many narrow ones.** The mart layer is built as wide, denormalized tables reusable across many dashboards — not one mart per report. A second wide mart (e.g., `revenue_detail` for Stripe + Fanbasis reconciliation) is added only when a genuinely different grain emerges (source: *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook).

---

## 7. Timeline

Target: dashboard in client's hands in **2–3 weeks** from kickoff.

| Week | Deliverable |
|---|---|
| **Week 0 (kickoff)** | Align with client on layered SLA thresholds; confirm GHL tag names for junk/DQ; access verification end-to-end |
| **Week 1** | Ingestion live for all 5 v1 sources (Typeform, Calendly, Stripe via Fivetran; GHL, Fanbasis via Python). Raw tables landing in BQ daily. |
| **Week 2** | Staging models, core facts (`fct_calls_booked`, `fct_sdr_outreach`), primary mart, data-quality tests passing, dashboard wired. |
| **Week 3** | Buffer + client iteration + documentation + handoff walkthrough |

---

## 8. Out of scope for v1 (tracked for v1.5 / v2)

- Fathom ingestion and show-rate / on-time-join metrics → **v1.5**
- Slack emoji-reaction claim time → **v2** (requires Slack Events API subscriber)
- Real-time alerts / reverse-ETL back into Slack or GHL → **v2**
- Revenue cohorting (LTV, payment-plan completion) → **v2**
- AE-side metrics (call-to-close velocity, closer leaderboards) → **v2**

---

## 9. Risks

| Risk | Mitigation |
|---|---|
| **Own bandwidth (solo build)** | AI-heavy execution with Claude Code; strict v1 scope; 3rd week is buffer |
| **GHL API schema surprises** | Start ingestion Day 1 so unknowns surface early; keep pulls incremental with a last-modified-since cursor |
| **"First touch" ambiguity on unbooked fills** | v1 ships primary SLA only; secondary/tertiary deferred until defined with client |
| **Rep pushback on data-quality diagnostic** | Position the gap metric as diagnostic, not performance — it protects reps by showing leadership that the logging system is the problem, not the people |
| **Fanbasis API limits / quirks** | Daily refresh, small volume — worst case fall back to CSV export if API proves unreliable |

---

## 10. What "v1 done" looks like

- Client can open the Looker Studio dashboard and see the five tiles, updated daily
- Numbers match leadership's gut check within tolerance, or the delta is explainable
- `dbt docs` site is generated and linked — every mart field has a description
- Ingestion runs on GitHub Actions cron without manual intervention
- A short Loom walkthrough (≤10 min) exists showing leadership how to read the dashboard

---

## 11. Template-ability (why this engagement matters)

This scope is deliberately structured so that client #2 (another high-ticket coaching business) can be stood up by forking the repo and swapping:
- API credentials
- GHL custom field / tag / stage name mappings
- Funnel-specific Typeform identifiers
- Rep / offer dimensions

Everything else — ingestion code, dbt models, tests, dashboard — travels. The productization bet is that this exact architecture works for the entire high-ticket coaching vertical with minor configuration.

**Schema-per-audience is the scaling lever.** As the product matures, the marts layer can be split into audience-specific schemas (e.g., `marts_sdr`, `marts_leadership`, `marts_finance`) with warehouse-level permissions controlling who sees what. One tenant can hide payment-plan details from the SDR-facing BI connection while exposing them to the CEO. The v1 client ships with a single `marts` schema, but the template repo reserves this schema-split as the intended path for multi-stakeholder or multi-tenant expansion (source: *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook).
