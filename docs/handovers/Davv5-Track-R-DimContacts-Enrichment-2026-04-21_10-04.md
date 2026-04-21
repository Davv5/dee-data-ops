# Session Handover — Track R: dim_contacts UTM + lead-magnet enrichment

**Branch:** `Davv5/Track-R-DimContacts-Enrichment`
**Timestamp:** `2026-04-21_10-04` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Populate six columns on `dim_contacts` that were NULL-stubbed during Track E so downstream marts (`sales_activity_detail`, `lead_journey`, `revenue_detail`) can surface actual attribution data instead of null-heavy columns. Columns in scope: `first_touch_campaign`, `first_touch_source`, `first_touch_medium`, `last_touch_campaign`, `last_touch_source`, `last_touch_medium`, `lead_magnet_first_engaged`, and `full_name` (currently an alias workaround — make it a real column).

## Changed files (expected)

```
dbt/models/warehouse/dimensions/dim_contacts.sql         — edited — add 8 enriched columns
dbt/models/warehouse/dimensions/_dimensions__models.yml  — edited — add column-level tests + descriptions
dbt/models/marts/sales_activity_detail.sql               — edited — remove NULL stubs, use real columns
dbt/models/marts/lead_journey.sql                        — edited — remove NULL stubs
dbt/models/marts/revenue_detail.sql                      — edited — remove NULL stubs
WORKLOG.md                                                — edited — dated entry with coverage %
```

## Tasks

- [ ] Confirm the source truth — where do UTM values live in raw_ghl? Expected in `raw_ghl.contacts.payload.customFields` as a list of `{ id, value }` objects. Check D-DEE's custom-field IDs for `utm_campaign`, `utm_source`, `utm_medium`:
      ```sql
      SELECT DISTINCT JSON_VALUE(f, '$.id') AS field_id, COUNT(*) rows
      FROM `dee-data-ops.raw_ghl.contacts` c,
           UNNEST(JSON_QUERY_ARRAY(c.payload, '$.customFields')) f
      WHERE JSON_VALUE(f, '$.id') IN (<candidate ids>)
      GROUP BY field_id
      ```
- [ ] Consult `stg_ghl__contacts.sql` — it may already parse these fields; if so, use them directly rather than re-parsing
- [ ] For first-touch vs last-touch: GHL doesn't natively store both. Proxy:
      - **first_touch_***: value of the field on the contact's `date_added` snapshot (if custom-field history is tracked) OR the current field value if it's never been overwritten
      - **last_touch_***: current field value on the contact record
      - If GHL doesn't track history (likely), **STOP AND ASK** — first/last may have to be the same column for v1, or David may want to keep them as NULLs with documentation. Don't guess.
- [ ] For `lead_magnet_first_engaged`: derive from GHL tags per `.claude/rules/warehouse.md` "lead-magnet attribution" section. Look for tags matching a lead-magnet taxonomy (investigate distinct tag values first):
      ```sql
      SELECT DISTINCT tag FROM `dee-data-ops.stg_ghl.contacts`, UNNEST(tags) AS tag
      WHERE tag ILIKE '%magnet%' OR tag ILIKE '%opt%in%'
      ```
- [ ] For `full_name`: coalesce `first_name || ' ' || last_name`, strip leading/trailing whitespace, null when both parts are null
- [ ] Add column-level `description:` + `tests:` (not_null where applicable) to the YAML
- [ ] Update the three marts that currently have `NULL as first_touch_campaign` etc. — replace with the real column reference
- [ ] Run `dbt build --target dev --select dim_contacts+` — all downstream must rebuild green
- [ ] Compute coverage: `SELECT COUNTIF(first_touch_campaign IS NOT NULL) / COUNT(*) FROM dim_contacts` and include in WORKLOG entry
- [ ] Append WORKLOG entry with coverage numbers per column
- [ ] Run `/handover`
- [ ] Commit locally

## Decisions already made

- **UTM fields come from custom_fields, not contact-level tags.** GHL's UTM capture lives in custom fields; tags are for categorical segmentation.
- **Keep NULL as the honest value for unattributable contacts.** Don't back-fill or synthesize. A contact with no UTM data gets NULL, and the downstream `attribution_quality_flag` already handles that case.
- **Do NOT delete the NULL-stub lines yet — replace them in place.** Makes the diff clearer.

## Open questions

- First-touch vs last-touch history: GHL custom-field history availability → **STOP AND ASK if unclear**
- Lead-magnet tag taxonomy: if the distinct-tags query returns an ambiguous mix, **STOP AND ASK** — David knows the taxonomy from the roster sheet

## Done when

- All eight columns populated on `dim_contacts`
- Three downstream marts no longer have NULL stubs for these fields
- `dim_contacts` coverage for UTM fields ≥ 40% (rough floor — if much lower, investigate)
- All existing tests green
- New column-level descriptions + not-null tests added
- WORKLOG entry with coverage numbers
- Commit sits locally

## Context links

- Track E handover (original dim_contacts): `docs/handovers/Davv5-Track-E-*.md`
- `.claude/rules/warehouse.md` — lead-magnet attribution section
- Corpus reference: *"Modern Data Stack Identity Spine"* in the Data Ops notebook (ask-corpus to pull the relevant section)
- Oracle UTM column: `Master Lead Sheet.xlsx` column M "Tracking Era" — divides pre-UTM vs UTM eras
