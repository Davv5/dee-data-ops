# Lead Magnet Mart Readout - 2026-05-02

## Contract

Mart: `lead_magnet_detail`

Grain: one row per GHL opportunity.

Primary key: `opportunity_id`.

Business question: which lead magnets create volume, get worked, book calls,
and turn into paid revenue?

Design choice: at D-DEE, GHL pipelines are the closest durable lead-magnet /
funnel lanes. Because many contacts enter more than one pipeline, the mart uses
an opportunity window for event attribution:

`opportunity_created_at <= event timestamp < next_opportunity_created_at`

That window attributes outreach, bookings, and revenue to the most recent
opportunity before the event, instead of over-crediting every pipeline a contact
has ever touched.

## Validation

Built in BigQuery dev schema as `dev_david.lead_magnet_detail`.

`dbt build --target dev_local --select lead_magnet_detail lead_magnet_detail_opportunity_parity`
passed 13 of 13 checks:

- 26,229 mart rows
- row-count parity with `stg_ghl__opportunities`
- unique and not-null `opportunity_id`
- not-null lead-magnet identifiers and metric columns
- accepted attribution quality flags

After adding `lead_magnet_pipeline_taxonomy`, the validation command became:

`dbt build --target dev_local --select lead_magnet_pipeline_taxonomy lead_magnet_detail lead_magnet_detail_opportunity_parity lead_magnet_detail_taxonomy_coverage`

It passed 23 of 23 checks:

- 36 taxonomy seed rows loaded
- all opportunity-bearing pipelines mapped to taxonomy
- no uncategorized opportunity-bearing pipeline
- mart row-count parity still holds
- category and attribution accepted-values tests passed

After adding `lead_magnet_offer_type`, the same command passed 27 of 27
checks with no warnings:

- offer type loaded on all 36 taxonomy seed rows
- offer type landed on every `lead_magnet_detail` row
- accepted-values tests protect seed and mart offer types

## First Readout

All opportunity windows:

- 26,229 opportunities
- 15,600 contacts
- 4,671 attributed bookings
- $275,228.16 net revenue after refunds
- 44.6% of contacts appear in more than one GHL pipeline
- 67.0% of opportunities belong to contacts with more than one pipeline

Top overall revenue lanes:

| Lead magnet / pipeline | Opportunities | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| Brand Scaling Blueprint Booked Calls | 2,653 | 1,253 | 475 | $167,431.12 | $63.11 |
| Dee Builds Brands MAIN Sales Pipeline | 4,600 | 400 | 55 | $20,681.03 | $4.50 |
| 12/15 Launch | 6,515 | 1,307 | 90 | $17,221.92 | $2.64 |
| Inner Circle 2.0 Launch | 117 | 43 | 24 | $16,279.39 | $139.14 |
| Inner Circle Launch | 233 | 67 | 17 | $15,018.04 | $64.46 |
| Speed to Lead Call | 161 | 56 | 30 | $12,366.89 | $76.81 |

## Taxonomy Readout

The initial taxonomy classifies every current opportunity-bearing pipeline. All
classifications are `inferred_from_name`, so this should be reviewed with David
before it becomes final operating truth.

| Category | Opportunities | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| Sales / operating pipeline | 7,418 | 1,709 | 560 | $200,479.04 | $27.03 |
| Launch / event | 6,921 | 1,430 | 131 | $48,519.35 | $7.01 |
| True lead magnet | 11,349 | 1,495 | 89 | $25,260.77 | $2.23 |
| Waitlist | 541 | 37 | 1 | $969.00 | $1.79 |

Top true lead magnets:

| True lead magnet | Opportunities | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| AI Brand Building Prompts | 4,250 | 706 | 18 | $5,051.43 | $1.19 |
| SMS Templates | 512 | 65 | 17 | $4,226.69 | $8.26 |
| Content Guide | 813 | 168 | 11 | $3,631.99 | $4.47 |
| Google Docs | 1,117 | 123 | 7 | $2,384.70 | $2.13 |
| Manufacturer List | 740 | 124 | 9 | $2,208.98 | $2.99 |
| Tariff Manufacturer List | 1,423 | 115 | 9 | $1,999.43 | $1.41 |

Top sales / operating pipelines:

| Sales / operating pipeline | Opportunities | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| Brand Scaling Blueprint Booked Calls | 2,653 | 1,253 | 475 | $167,431.12 | $63.11 |
| Dee Builds Brands Main Sales Pipeline | 4,600 | 400 | 55 | $20,681.03 | $4.50 |
| Speed-to-Lead Call | 161 | 56 | 30 | $12,366.89 | $76.81 |

## Offer Type Readout

`lead_magnet_offer_type` answers the creative/package question that category
does not: prompts vs templates vs guides vs lists vs trainings.

All dashboard-included lanes:

| Offer type | Opportunities | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| Sales pipeline | 7,418 | 1,709 | 560 | $200,479.04 | $27.03 |
| Launch / event | 6,921 | 1,430 | 131 | $48,519.35 | $7.01 |
| Guide / doc | 2,084 | 305 | 19 | $6,116.69 | $2.94 |
| Template | 781 | 87 | 21 | $5,309.79 | $6.80 |
| Prompt pack | 4,250 | 706 | 18 | $5,051.43 | $1.19 |
| Resource list | 2,163 | 239 | 18 | $4,208.41 | $1.95 |
| Training / class | 352 | 51 | 5 | $2,202.30 | $6.26 |
| Community | 696 | 79 | 5 | $1,984.90 | $2.85 |
| Waitlist | 541 | 37 | 1 | $969.00 | $1.79 |
| Video / replay | 113 | 13 | 2 | $342.25 | $3.03 |
| Giveaway | 780 | 15 | 1 | $45.00 | $0.06 |
| Uncategorized | 130 | 0 | 0 | $0.00 | $0.00 |

True lead magnets only:

| Offer type | Opportunities | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| Guide / doc | 2,084 | 305 | 19 | $6,116.69 | $2.94 |
| Template | 781 | 87 | 21 | $5,309.79 | $6.80 |
| Prompt pack | 4,250 | 706 | 18 | $5,051.43 | $1.19 |
| Resource list | 2,163 | 239 | 18 | $4,208.41 | $1.95 |
| Training / class | 352 | 51 | 5 | $2,202.30 | $6.26 |
| Community | 696 | 79 | 5 | $1,984.90 | $2.85 |
| Video / replay | 113 | 13 | 2 | $342.25 | $3.03 |
| Giveaway | 780 | 15 | 1 | $45.00 | $0.06 |
| Uncategorized | 130 | 0 | 0 | $0.00 | $0.00 |

First opportunity view, for acquisition quality:

| First-touch lead magnet / pipeline | First-touch opps | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| 12/15 Launch | 5,827 | 1,097 | 64 | $11,208.40 | $1.92 |
| Brand Scaling Blueprint Booked Calls | 251 | 70 | 26 | $9,001.60 | $35.86 |
| Dee Builds Brands MAIN Sales Pipeline | 917 | 104 | 17 | $6,714.20 | $7.32 |
| Inner Circle Launch | 117 | 17 | 4 | $5,595.00 | $47.82 |
| Inner Circle 2.0 Launch | 37 | 19 | 3 | $1,210.28 | $32.71 |

Latest opportunity view, for current operating state:

| Latest lead magnet / pipeline | Latest opps | Bookings | Paid opps | Net revenue | Revenue / opp |
|---|---:|---:|---:|---:|---:|
| Dee Builds Brands MAIN Sales Pipeline | 4,048 | 275 | 48 | $18,317.10 | $4.52 |
| Speed to Lead Call | 141 | 55 | 27 | $11,300.99 | $80.15 |
| Brand Scaling Blueprint Booked Calls | 1,388 | 618 | 46 | $7,754.82 | $5.59 |
| 12/15 Launch | 4,352 | 45 | 12 | $3,469.00 | $0.80 |
| AI Brand Building Prompts | 1,612 | 36 | 4 | $1,531.02 | $0.95 |

## Interpretation

`Brand Scaling Blueprint Booked Calls` is the clearest money lane right now:
high bookings, high paid count, and the largest net revenue by a wide margin.

`12/15 Launch` is a volume machine, but not a strong paid-conversion lane in
its current form. It deserves segmentation before anyone calls it bad: the
first-touch view says it creates a lot of bookers, but revenue per opportunity
is low.

Inside true lead magnets, templates are the strongest signal so far: lower
volume than prompts, but better paid count and much higher revenue per
opportunity. Prompt packs drive the most booking volume, but revenue per
opportunity is low. Giveaways are the weakest tested offer type in this data.

`Speed to Lead Call`, `Inner Circle 2.0 Launch`, and `Inner Circle Launch` are
small-volume, high-value lanes. They should not be judged by volume alone.

`Dee Builds Brands MAIN Sales Pipeline` looks more like an operating/sales
pipeline than a true acquisition magnet. It is valuable, but it should be
classified separately before the dashboard compares it against true opt-in
magnets.

Several lanes show bookings and revenue with little or no attributed GHL
outreach. Treat that as a source-truth question, not an automatic sales-team
failure. It may mean the conversion happened through Calendly/self-serve,
another communication channel, or a missing conversation capture path.

## Next Best Move

Review the initial lead-magnet taxonomy with David:

- true lead magnet
- launch/event
- waitlist
- sales/operating pipeline
- internal/test/retired
- offer type labels, especially the generic `Lead Magnet` row currently marked
  as `uncategorized`

Then build the dashboard chapter so it answers the money question cleanly:

1. Which true magnets create qualified opportunities?
2. Which magnets book calls?
3. Which magnets convert to paid revenue?
4. Which magnets leak because they are not worked?
5. Which magnets are high-value but low-volume and deserve more traffic?
