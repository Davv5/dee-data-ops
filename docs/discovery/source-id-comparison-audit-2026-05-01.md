# Source-ID Comparison Audit

Date: 2026-05-01.

Purpose: compare current-project raw IDs against legacy-project raw IDs before pausing any legacy jobs. This is the follow-up to the duplicate-data and legacy-runtime audits.

This audit is read-only. No BigQuery tables, Cloud Run jobs, or Scheduler jobs were changed.

Companion docs:

- `docs/discovery/cloud-project-provenance-map.md`
- `docs/discovery/duplicate-data-audit-2026-05-01.md`
- `docs/discovery/legacy-runtime-audit-2026-05-01.md`
- `docs/discovery/current-data-layer-truth-map.md`

## Short Answer

The current consolidated project is strong enough for dashboard work, but not every legacy stream is safe to pause.

Safe or near-safe:

- GHL cold objects: contacts, opportunities, users, pipelines
- Stripe core objects: charges, customers, refunds
- Calendly core objects: events, invitees, event types
- Typeform forms

Not safe to pause yet:

- `ghl-hot` / GHL conversations and messages
- Typeform historical responses, if older response history still matters

Best first pause candidate:

```text
calendly-poll
```

Reason: current Calendly has 100% coverage for scheduled events via legacy `event`, 100% coverage for invitees, 92.31% current coverage for event types, and current invitee payloads embed `questions_and_answers` on all 5,488 invitee rows. The legacy poller table `raw_calendly.scheduled_events` is only a 258-ID subset and is already producing quota/timeouts.

Do not pause `ghl-hot` yet.

## Method

The comparisons dedupe each side to distinct source IDs and full-outer-join current vs legacy IDs.

Current project:

```text
project-41542e21-470f-4589-96d
```

Legacy projects:

```text
dee-data-ops
dee-data-ops-prod
```

Current raw sources checked:

- `Raw.ghl_objects_raw`
- `raw_ghl.ghl__messages_raw`
- `raw_ghl.ghl__conversations_raw`
- `Raw.calendly_objects_raw`
- `Raw.stripe_objects_raw`
- `Raw.typeform_objects_raw`

Legacy raw sources checked:

- `dee-data-ops.raw_ghl.*`
- `dee-data-ops.raw_calendly.*`
- `dee-data-ops.raw_stripe.*`
- `dee-data-ops.raw_typeform.*`

## GHL Cold Objects

Comparison: current `Raw.ghl_objects_raw` vs legacy `dee-data-ops.raw_ghl.{contacts,opportunities,users,pipelines}`.

| Object | Current IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current |
|---|---:|---:|---:|---:|---:|---:|---:|
| contacts | 16,095 | 16,132 | 16,095 | 0 | 37 | 100.00% | 99.77% |
| opportunities | 26,219 | 26,416 | 26,219 | 0 | 197 | 100.00% | 99.25% |
| pipelines | 36 | 36 | 36 | 0 | 0 | 100.00% | 100.00% |
| users | 16 | 16 | 16 | 0 | 0 | 100.00% | 100.00% |

Classification:

```text
ghl-cold is a pause candidate after one final freshness/consumer check
```

Read: the current project contains every current distinct ID observed for the main cold objects. Legacy has a small tail of extra contacts/opportunities. That tail may be historical/deleted/stale, but it should be sampled before pausing.

## GHL Hot Objects

There are two possible current-project places to compare:

1. `Raw.ghl_objects_raw`, which contains current `conversations` but not `messages`.
2. `raw_ghl.ghl__conversations_raw` and `raw_ghl.ghl__messages_raw`, the current-project legacy-shaped copies.

### Current Unified Raw vs Legacy

Comparison: current `Raw.ghl_objects_raw` conversations vs legacy `dee-data-ops.raw_ghl.conversations`.

| Object | Current IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current |
|---|---:|---:|---:|---:|---:|---:|---:|
| conversations | 1,662 | 15,769 | 1,662 | 0 | 14,107 | 100.00% | 10.54% |

### Current Legacy-Shaped Raw vs Legacy

Comparison: current `project-41542e21-470f-4589-96d.raw_ghl.*` vs legacy `dee-data-ops.raw_ghl.*`.

| Object | Current raw_ghl IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current raw_ghl |
|---|---:|---:|---:|---:|---:|---:|---:|
| conversations | 101 | 15,769 | 101 | 0 | 15,668 | 100.00% | 0.64% |
| messages | 17,971 | 46,620 | 17,960 | 11 | 28,660 | 99.94% | 38.52% |

Classification:

```text
ghl-hot is not safe to pause yet
```

Read: legacy has a much larger conversations/messages estate than the current project. This may include old history no longer needed by the current dashboard, but it is exactly the kind of raw-history stream that should be migrated or archived deliberately before pausing.

## Calendly

Comparison: current `Raw.calendly_objects_raw` vs legacy `dee-data-ops.raw_calendly`.

Initial comparison against the legacy poller table showed that `raw_calendly.scheduled_events` is only a 258-ID subset:

| Legacy table | Current scheduled event IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current |
|---|---:|---:|---:|---:|---:|---:|---:|
| `scheduled_events` poller | 5,488 | 258 | 258 | 5,230 | 0 | 4.70% | 100.00% |

The full legacy event table is `raw_calendly.event`, not the poller subset:

| Legacy table | Current scheduled event IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current |
|---|---:|---:|---:|---:|---:|---:|---:|
| `event` | 5,488 | 5,488 | 5,488 | 0 | 0 | 100.00% | 100.00% |

Other Calendly objects:

| Object | Current IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current |
|---|---:|---:|---:|---:|---:|---:|---:|
| event_invitees | 5,488 | 5,488 | 5,488 | 0 | 0 | 100.00% | 100.00% |
| event_types | 13 | 17 | 12 | 1 | 5 | 92.31% | 70.59% |

Calendly Q&A detail check:

| Current invitee rows | Rows with `questions_and_answers` | Rows with `questions_and_responses` | Rows with `text_reminder_number` |
|---:|---:|---:|---:|
| 5,488 | 5,488 | 0 | 5,488 |

Classification:

```text
calendly-poll is the first safe pause candidate, after creating a rollback note
```

Read: core event and invitee coverage is complete in the current project. The legacy normalized `question_and_answer` child table is not the only copy of Q&A detail; current invitee payloads embed `questions_and_answers`. The legacy poller is also the job with observed timeout/quota strain.

## Stripe

Comparison: current `Raw.stripe_objects_raw` vs legacy `dee-data-ops.raw_stripe`.

| Object | Current IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current |
|---|---:|---:|---:|---:|---:|---:|---:|
| charges | 3,375 | 3,375 | 3,375 | 0 | 0 | 100.00% | 100.00% |
| customers | 516 | 516 | 516 | 0 | 0 | 100.00% | 100.00% |
| refunds | 21 | 21 | 21 | 0 | 0 | 100.00% | 100.00% |

Classification:

```text
legacy Stripe core objects are fully represented for the checked entities
```

Read: Stripe is historical-only for the business direction, but the core checked objects line up exactly by ID.

## Typeform

Comparison: current `Raw.typeform_objects_raw` vs legacy `dee-data-ops.raw_typeform`.

| Object | Current IDs | Legacy IDs | IDs in both | Current-only | Legacy-only | Current IDs in legacy | Legacy IDs in current |
|---|---:|---:|---:|---:|---:|---:|---:|
| forms | 126 | 126 | 126 | 0 | 0 | 100.00% | 100.00% |
| responses | 5,014 | 22,423 | 5,014 | 0 | 17,409 | 100.00% | 22.36% |

Classification:

```text
forms are covered; response history is not fully represented in current raw
```

Read: current has every response it has in legacy, but legacy has a much larger response-history tail. This does not block dashboard Speed-to-Lead, but it matters before any Typeform decommission work.

## Pause Safety Matrix

| Stream/job | Classification | Why |
|---|---|---|
| `calendly-poll` | **First pause candidate** | Current has complete event/invitee coverage; Q&A detail is embedded in current invitee payloads; legacy poller is noisy and erroring. |
| `ghl-cold` | **Pause candidate after sample check** | Current covers all checked current IDs for contacts/opportunities/users/pipelines; legacy has a small extra historical tail. |
| `ghl-hot` | **Do not pause yet** | Legacy has far more messages/conversations than current. |
| legacy Stripe core | **Safe as reference/archive** | Checked core IDs match 100%. |
| legacy Typeform forms | **Covered** | Form IDs match 100%. |
| legacy Typeform responses | **Keep/reference until history decision** | Legacy has 17,409 response IDs not in current. |

## Recommended Next Action

Pause only one thing first, and make it reversible:

```text
pause dee-data-ops-prod scheduler job calendly-poll
```

Recommended pre-pause checklist:

1. Capture current scheduler/job state in a small runbook.
2. Confirm dashboard/dbt current path reads `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`, not `dee-data-ops.raw_calendly`.
3. Pause only Scheduler job `calendly-poll`, not the Cloud Run Job definition.
4. Watch for one hour:
   - current `Raw.calendly_objects_raw` freshness remains healthy
   - current `Marts.mrt_speed_to_lead_*` still refreshes
   - no dashboard contract depends on legacy `raw_calendly.scheduled_events`
5. Keep rollback command ready:

```bash
gcloud scheduler jobs resume calendly-poll --location=us-central1 --project=dee-data-ops-prod
```

Do not pause `ghl-hot` until the legacy messages/conversations history is migrated, archived, or explicitly declared unnecessary.
