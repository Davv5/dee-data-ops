# Business Question Map — Data Discovery Sprint

**Purpose:** This page answers a simple question: "What should the dashboards help D-DEE decide?"

Last updated: 2026-04-25.

This page connects to:

- `docs/discovery/source-inventory.md` — what data we have and whether it is fresh
- `docs/discovery/staging-models.md` — which raw data has been cleaned up enough for dbt to use
- `docs/discovery/gap-analysis.md` — what is missing or broken
- `docs/discovery/coverage-matrix.md` — the next page; it will show which business questions each data source can answer

---

## How to use this page

This is not a dashboard yet. It is the map we use before building more dashboards.

For each business question, this page shows:

- **Who cares** — every role or team that would consume the answer.
- **Owner** — the single function at D-DEE whose primary KPI moves with the answer. Inferred from the playbook chapter (Funnel / Attribution / Conversion / Net Revenue / Retention / Conversation Intelligence). "Who cares" can be a list; "Owner" is exactly one. The named individual sits behind the role and is not tracked here because the team is large enough that role-level routing is the actionable unit.
- **Decision it helps make** — what the client can actually do with the answer.
- **Data we need** — which systems must be connected and trustworthy.
- **Where we stand today** — whether we can answer it now.
- **Priority** — how important it is for the next build.

### Plain-English cheat sheet

| Project word | Plain meaning |
|---|---|
| Raw data | The untouched data that lands from tools like GHL, Calendly, Fanbasis, Typeform, Fathom, and Stripe. |
| Cleaned-up table | A simple table our data-cleaning tool, dbt, can use after we clean names, dates, IDs, and messy fields. Engineers may call this a staging model. |
| Dashboard-ready table | A table shaped for dashboards and client reporting. Engineers may call this a mart. |
| Next dashboard build list | The ordered list of dashboard-ready tables and dashboards we will build next. Engineers may call this the Gold-layer roadmap. |
| Which GHL copy we trust | We have more than one GHL data path. Before building more GHL dashboards, we need to decide which copy is the source of truth. |
| Stale data | Data that has not updated recently enough to trust for current decisions. |
| Blocked | We cannot safely answer the question yet because important data is missing, stale, or not cleaned up. |
| Owner | The function at D-DEE whose primary KPI moves with the answer (e.g., SDR Manager, Sales Manager, Marketing Lead, Finance Lead). The named individual sits behind the role; the role is what the dashboard is routed to. |

### Priority labels

- **Start here (P0)** — this unlocks multiple important decisions.
- **Next (P1)** — important, but depends on a Start here item or is less urgent.
- **Later (P2)** — useful after the core reporting system is stable.

### Status labels

- **Working** — we can answer this reliably enough today.
- **Partly working** — we can answer some of it, but important pieces are missing.
- **Blocked** — we should not trust the answer until a specific issue is fixed.
- **Not started** — no confirmed data source exists yet.

---

## Business questions we need to answer

| Business question | Who cares | Owner | Decision it helps make | Data we need | Where we stand today | Priority |
|---|---|---|---|---|---|---|
| How fast do we contact booked leads? | D-DEE leadership, SDR manager, Precision Scaling | **SDR Manager** | Which setter, pipeline, or lead source needs attention because leads are not getting contacted fast enough? | Calendly bookings, GHL conversations, GHL contacts, GHL opportunities, GHL pipelines, setter roster | **Partly working.** The first Speed-to-Lead dashboard is live, but GHL is stale and one GHL conversation table is missing many rows compared with the older GHL copy. | Start here (P0) |
| Which leads turn into paid customers? | D-DEE leadership, finance, marketing, Precision Scaling | **D-DEE Leadership** | Which campaigns, forms, offers, and closers are creating real revenue? | Fanbasis, historical Stripe, GHL contacts and opportunities, Typeform, Calendly, matching payments to contacts | **Blocked.** Fanbasis is the live payment source, but dbt does not have a cleaned-up Fanbasis transactions table yet. Stripe is only useful for old history. | Start here (P0) |
| Which setters are creating bookings, and where are handoffs breaking? | SDR manager, D-DEE leadership | **SDR Manager** | Where should setter coaching, staffing, or follow-up rules change? | GHL contacts, conversations, opportunities, users, tasks/messages if available, Calendly, setter roster | **Partly working to blocked.** Contacts and opportunities exist but are stale. Conversations are undercounted. Messages, users, and tasks are empty in the source data. Some roster roles are still missing. | Start here (P0) |
| Which forms and channels bring good leads? | Marketing owner, D-DEE leadership, Precision Scaling | **Marketing Lead** | Which lead sources should get more spend, more attention, or less attention? | Typeform responses and forms, GHL contacts, GHL opportunities, Calendly bookings, Fanbasis, historical Stripe | **Partly working.** Typeform responses are fresh, but we do not reliably know which form each response came from yet. Revenue follow-through is blocked until Fanbasis is cleaned up for dbt. | Start here (P0) |
| Which closers turn calls into customers? | Sales manager, closers, D-DEE leadership | **Sales Manager** | Which closers need coaching, which offers are converting, and where are sales calls breaking down? | GHL opportunities, GHL users, Fathom calls and transcripts, Fanbasis, Calendly | **Partly working.** Fathom call basics exist, but transcripts are missing. GHL users are empty in the source data. Fanbasis is not cleaned up for dbt yet. | Next (P1) |
| Where do leads get stuck in the sales process? | Sales ops, SDR manager, closers | **Sales Operations** | Which sales stage or owner is slowing the pipeline down? | GHL opportunities, GHL pipeline stages, GHL contacts, Calendly | **Partly working.** GHL opportunity and pipeline data exists but is stale. We still need to confirm whether we have enough stage history to measure time stuck in each step. | Next (P1) |
| Which booked calls no-show, and what rescue follow-up works? | SDR manager, sales ops | **SDR Manager** | Where should the team add reminders, rescue calls, or follow-up capacity? | Calendly events and invitees, Calendly status fields, GHL conversations, GHL opportunities, GHL tasks/messages if available | **Partly working.** Calendly is fresh, but no-show, cancel, and reschedule details are not fully pulled into the cleaned-up tables. GHL follow-up data is stale and incomplete. | Next (P1) |
| Where are refunds and chargebacks coming from? | Finance, D-DEE leadership, customer success | **Finance Lead** | Which offers, cohorts, or closers are creating refund risk? | Fanbasis refunds/chargebacks if available, Fanbasis transactions, historical Stripe refunds/disputes, GHL contacts and opportunities | **Blocked.** We know Fanbasis transactions exist, but we do not yet know whether refunds and chargebacks are landing. Stripe has old refund/dispute data, but it is stale and not fully cleaned up. | Next (P1) |
| Which customers stick around, and which ones churn? | D-DEE leadership, customer success, finance | **D-DEE Leadership** | Which acquisition paths or sales patterns lead to better long-term customers? | Fanbasis subscriptions/customers if available, Fanbasis transactions, historical Stripe subscriptions, GHL contacts and opportunities | **Blocked.** We do not yet know whether Fanbasis includes enough customer or subscription data. Right now we only know transactions are landing. | Next (P1) |
| Which lead sources create bad or unusable leads? | SDR manager, marketing owner, data ops | **SDR Manager** | Which leads should be filtered, deprioritized, or treated differently? | Typeform responses, GHL contacts, GHL opportunities, Calendly, agreed lead-quality rules | **Partly working.** The Speed-to-Lead dashboard has one agreed filter, but broader lead quality needs better form tracking and fresher GHL data. | Next (P1) |
| Which content, ads, or funnels actually make money? | Marketing owner, D-DEE leadership | **Marketing Lead** | Which funnel paths deserve more spend and which should be cut? | Typeform, GHL contacts and opportunities, Calendly, Fanbasis, campaign fields where available | **Blocked to partly working.** Some campaign fields are placeholders today. Typeform form tracking and Fanbasis cleanup are both needed before this can be trusted. | Later (P2) |
| What is happening on sales calls? | Sales manager, closers, coaching lead | **Sales Manager** | Which objections, talk tracks, or call habits are tied to revenue or refunds? | Fathom transcripts, Fathom call basics, GHL opportunities, Fanbasis | **Blocked.** Fathom call basics are usable, but transcripts are missing for all calls. Without transcripts, we cannot analyze call content. | Later (P2) |
| Should Slack be used as data, or only for alerts? | Precision Scaling, D-DEE ops | **David / Operations** | Should Slack become part of reporting, stay only as an alert channel, or be ignored for this project? | Slack, only if David confirms it should be a data source | **Not started.** Current evidence says Slack is only an alert destination, not a data source. We should not add it to the next matrix unless David says Slack data matters. | Later (P2) |

---

## Big decisions we need before building more dashboards

These are the main choices that should be written into the next dashboard build list before new dashboard work starts.

### 1. Fanbasis is the live revenue unlock

**Plain question:** Which Fanbasis data do we need first?

At minimum, we need Fanbasis transactions cleaned up for dbt. After that, we need to decide whether refunds, chargebacks, products, customers, subscriptions, or plans matter for the first revenue dashboards.

**Why this matters:** Fanbasis is where current payments live. Without it, current revenue reporting is blocked or forced to use old Stripe history.

**Recommended next move:** After this discovery phase, inspect the Fanbasis transaction data, create a cleaned-up Fanbasis transactions table, then decide which extra Fanbasis data is worth pulling in.

### 2. We need to choose which GHL data copy we trust

**Plain question:** Which GHL data should the dashboards trust?

Right now, GHL has more than one copy of the data. One copy is newer but missing important pieces. The older copy appears broader for conversations, but it is also stale.

**Why this matters:** GHL powers Speed-to-Lead, SDR performance, pipeline movement, no-show rescue, lead quality, and parts of revenue reporting. If we pick the wrong GHL copy, the dashboards may look polished but be wrong.

**Recommended next move:** Decide the trusted GHL path before building any new GHL-heavy dashboard. If the older copy is the best source, plan the work to point the cleaned-up GHL tables there. If the newer copy stays, fixing GHL freshness and missing pieces becomes a hard requirement.

### 3. Typeform needs better form tracking

**Plain question:** Do we need to know which form each lead came from?

Typeform responses are coming in, but the cleaned-up response table does not reliably show the form ID. There is also no cleaned-up Typeform forms table yet.

**Why this matters:** Without form tracking, we can count leads but cannot clearly explain which form, funnel, or offer produced them.

**Recommended next move:** Add a cleaned-up Typeform forms table as a low-risk helper. Only fix the deeper Typeform data-pull issue if the next dashboard build list needs form-level reporting.

### 4. Fathom call transcripts depend on whether call content matters now

**Plain question:** Do we need call-content analysis in the first next build, or can we wait?

Fathom has basic call information, but transcripts are missing. Basic call volume can be used now. Objection handling, talk-track quality, sentiment, and coaching analysis cannot.

**Why this matters:** Transcript work could be valuable, but it should not jump ahead of revenue and GHL trust unless call-content reporting becomes a top priority.

**Recommended next move:** Keep transcript repair as a later build item unless the next dashboard build list ranks call-content reporting near the top.

### 5. Slack should stay out unless we decide it is truly data

**Plain question:** Is Slack something we want to report on, or just a place alerts might be sent?

Current project evidence says Slack is an alert destination, not a data source.

**Why this matters:** If we include Slack without a clear reason, we create fake scope and distract from the data that already matters.

**Recommended next move:** Default to "Slack is not a data source" unless David specifically wants Slack messages or client comms included.

---

## What this means for the next build

The next dashboard build list should probably focus on three simple groups:

1. **Revenue** — get Fanbasis transactions cleaned up, then make current revenue visible.
2. **Sales operations** — choose the trusted GHL data, then make setter, speed-to-lead, and pipeline reporting more reliable.
3. **Attribution** — connect Typeform, Calendly, GHL, and Fanbasis so we can explain which lead paths turn into money.

Everything else should wait for a clear reason:

- Fathom transcript work waits until call-content reporting is ranked high enough.
- Stripe cleanup waits until old Stripe history is explicitly needed.
- Slack waits until David confirms it should be treated as data.
- New dashboards wait until the next dashboard build list names the table and business question.

---

## Next page to write

`docs/discovery/coverage-matrix.md`

Use this page as the list of rows. Use these confirmed data sources as columns:

- Calendly
- GHL
- Fanbasis
- Typeform
- Fathom
- Stripe
- Slack only if David confirms it is a data source

Each cell should say:

- what we can answer today
- what we need to answer
- what is missing
