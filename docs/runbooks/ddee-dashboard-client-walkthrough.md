# D-DEE Dashboard Client Walkthrough

Use this for a live client walkthrough of the D-DEE dashboard.

## Positioning

This dashboard is an operating surface for seeing where leads turn into money,
where follow-up leaks, and which customers need attention. It is powered by live
BigQuery marts, not exported spreadsheets.

The simple promise:

> We can now connect the journey from lead source to sales follow-up to purchase
> to retention, and we can click into the person-level evidence behind the
> numbers.

## Access

- Live dashboard: `https://dee-dashboard-mjxxki4snq-uc.a.run.app`
- Demo password: share separately, not in docs or email threads.
- Current auth: temporary demo password gate.
- Final auth direction: magic-link or role-based access later.

## Pre-Meeting Checklist

1. Open the dashboard in a clean browser window.
2. Log in before the call starts.
3. Open these tabs ahead of time:
   - `/speed-to-lead`
   - `/lead-magnets`
   - `/revenue`
   - `/retention?range=all`
4. Have one Customer 360 example ready from Retention or Revenue.
5. Do not demo writeback buttons unless the meeting goal is internal ops.

## Suggested Agenda

1. Start with the business question.
2. Show Speed-to-Lead as the operating baseline.
3. Show Lead Magnets as the source-to-revenue view.
4. Show Revenue as the money truth surface.
5. Show Retention as the recovery / expansion surface.
6. Click into Customer 360 to prove the dashboard is inspectable.
7. Close with what is live now and what comes next.

## Opening Script

> The goal of this dashboard is not just to display numbers. The goal is to make
> the sales and growth system inspectable. We want to know which leads are being
> worked, which lead magnets are producing buyers, how revenue is collected, and
> where there are follow-up or retention opportunities.

> The important part is that the dashboard is live against BigQuery. So when we
> look at a number, we can click into the supporting customer or source context
> instead of treating the number as a dead-end metric.

## Tab 1: Speed-to-Lead

### What To Say

> This is the response-health view. It answers: are leads actually being worked,
> who is reaching them, and where are we leaking follow-up?

### What To Point At

- Lead events worked.
- Reached by phone.
- Still not worked.
- Bookings within the SLA.
- Reached-by identity.
- Leak snapshot.

### Interpretation

Frame this as team execution, not blame.

Good phrasing:

> The goal is to reduce the unknowns and make the follow-up path visible.

Avoid:

> This rep failed.

## Tab 2: Lead Magnets

### What To Say

> This view connects lead magnets to opportunity behavior and buyer outcomes.
> It helps us understand which entry points are creating value and which ones are
> just creating volume.

### What To Point At

- Top magnets by volume.
- Buyer-producing magnets.
- Latest magnet before purchase.
- Taxonomy / classification.
- Revenue opportunity or quality gaps.

### Interpretation

This tab is about source quality.

Good phrasing:

> We are not only asking which magnet gets leads. We are asking which magnet
> creates worked leads, booked calls, buyers, and repeat value.

## Tab 3: Revenue

### What To Say

> This is the money truth surface. It shows collected revenue, payment behavior,
> product attribution, and where revenue credit is strong or still thin.

### What To Point At

- Collected net revenue.
- Buyers.
- Payment-plan buyers.
- Product family.
- Revenue credit / attribution gaps.
- Canceled booking recovery.

### Important Boundary

Revenue credit is not the same as current owner.

Say:

> Revenue credit means best-known sales attribution. It does not automatically
> mean this person owns the next follow-up today.

## Tab 4: Retention

### What To Say

> This is where the dashboard starts showing money still on the table. We can
> see failed plans, due payments, manual collection patterns, completed plans,
> and upsell candidates.

### What To Point At

- Recovery queue.
- Failed plans.
- Due now.
- Manual collections.
- One-time upsell candidates.
- Contract terms review.

### Interpretation

This is the strongest client-value story:

> The dashboard is not just asking what happened. It is helping identify where
> follow-up could protect or create revenue.

## Customer 360

### What To Say

> When a number raises a question, Customer 360 is where we inspect the actual
> person-level story.

### What To Point At

- Contact and customer identity.
- Lifetime net.
- Revenue credit.
- Payment history.
- Relationship timeline.
- Open operator actions.
- Lead magnet trail.
- Retention timeline.
- Sales-call contract evidence when available.

### Best Demo Move

Open a customer from Retention, then explain:

> This is why the dashboard is useful: we can go from a cohort number to the
> exact customer context behind it.

## Actions Tab

Actions exists, but keep it light unless the client asks.

Say:

> This is an early action queue. It is meant to become the place where the team
> or an AI action-handler can see who needs attention. For today, the main demo
> is the reporting and Customer 360 context.

## Truth Boundaries

Be explicit about these. It builds trust.

- The dashboard uses live BigQuery marts.
- Customer-level revenue is based on modeled payment facts.
- Payment-plan balance is not yet a full receivables ledger.
- Transcript contract terms are evidence, not final contract truth until reviewed.
- Current owner is intentionally unknown until a real ownership source is modeled.
- Revenue credit means attribution, not automatic current responsibility.

## Questions You May Get

### Is this live data?

Yes. The dashboard reads from BigQuery marts. Each page shows freshness status.

### Can we click into the numbers?

Yes. Customer 360 is the drilldown layer for person-level evidence.

### Can reps use this daily?

Yes for visibility. For direct task management, the Actions tab and writeback
flows are still early and should be treated as demo/ops foundations.

### Why are some owners unknown?

Because we are not inventing accountability. We can show revenue credit and
setter evidence, but current follow-up ownership needs a clean source before it
should be displayed as truth.

### What changed from the old dashboard idea?

This became a live operator app instead of a static report. It still follows the
simple dashboard philosophy, but it needs server-side BigQuery, routes, customer
drilldowns, and login.

## Strong Closing

> What we have now is the foundation for an operating system around revenue:
> where leads come from, how fast they are worked, what turns into cash, and
> where follow-up or retention money is still available.

## Recommended Next Step

After the demo, pick one operational question to improve next:

1. Sales Call Outcome mart: connect booked calls to shows, closes, and collected
   revenue.
2. Current Ownership source: model who should follow up now.
3. Retention action workflow: turn recovery queues into tracked actions.
