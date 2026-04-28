-- dim_offer.sql
-- P0-3: dim_offer Design | P0-4: Offer Mapping Rulebook
--
-- Canonical offer keys (frozen):
--   eba_barbershop_system | eba_membership | roc_core | roc_inner_circle | mike_parenting | unknown_offer
--
-- SCD policy: Type 1 — full replace on each pipeline refresh.
-- Mapping rule history is tracked in git, not in BigQuery.
-- Resolution order: booking_url (1) > event_name (3) > pipeline_name (4) > regex_utm_campaign (5) > regex_source (6) > unknown_offer
-- Conflict policy: highest priority wins; ties broken by longest rule_value; hard tie → unknown_hard_conflict.

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 1: Mapping rules
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_offer_mapping_rules` AS
SELECT rule_type, rule_value, canonical_offer, mapping_priority, is_regex, notes FROM (
  -- Priority 1: exact booking URL
  SELECT 'booking_url' AS rule_type, 'https://calendly.com/mindofdee/roc-strategy-call'                                          AS rule_value, 'roc_core'         AS canonical_offer, 1 AS mapping_priority, FALSE AS is_regex, 'ROC Strategy Call (mindofdee)' AS notes
  UNION ALL SELECT 'booking_url', 'https://calendly.com/dee-deesinnercircle/accelerator-1-on-1-strategy-call-clone',               'roc_core',         1, FALSE, 'ROC 1 on 1 Strategy Call (dee IC)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/houssam-precisionscaling/30min',                                           'roc_core',         1, FALSE, 'Closer 30min — Houssam (precisionscaling)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/jordan-precisionscaling/30min',                                            'roc_core',         1, FALSE, 'Closer 30min — Jordan (precisionscaling)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/kevin-precisionscaling/30min',                                             'roc_core',         1, FALSE, 'Closer 30min — Kevin (precisionscaling)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/jake-precisionscaling/30min',                                              'roc_core',         1, FALSE, 'Closer 30min — Jake (precisionscaling)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/dee-deesinnercircle/scale',                                                'roc_inner_circle', 1, FALSE, 'Brand Scaling Blueprint Access (dee IC)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/mindofdee/brand-scaling-blueprint-access-call',                            'roc_inner_circle', 1, FALSE, 'Brand Scaling Blueprint Access Call — Hammad (mindofdee)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/dee-deesinnercircle/brand-scaling-blueprint-interview',                    'roc_inner_circle', 1, FALSE, 'Brand Scaling Blueprint Interview (dee IC)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/dee-deesinnercircle/inner-circle-2',                                       'roc_inner_circle', 1, FALSE, 'Inner Circle Private Interview (dee IC)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/dee-deesinnercircle/brand-scaling-blueprint-access-clone-1',               'roc_inner_circle', 1, FALSE, 'Check Up Call (dee IC)'
  UNION ALL SELECT 'booking_url', 'https://calendly.com/dee-deesinnercircle/brand-scaling-blueprint-access-clone-2',               'roc_inner_circle', 1, FALSE, 'Reschedules Brand Scaling Blueprint Access (dee IC)'

  -- Priority 3: exact event name (used when scheduling_url is missing)
  UNION ALL SELECT 'event_name', 'ROC Strategy Call',                                     'roc_core',         3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'ROC 1 on 1 Strategy Call',                              'roc_core',         3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'Brand Scaling Blueprint Access',                         'roc_inner_circle', 3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'Brand Scaling Blueprint Access Call (Hammad)',           'roc_inner_circle', 3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'Brand Scaling Blueprint Interview',                      'roc_inner_circle', 3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'Accelerator 1 on 1 Strategy Call',                      'roc_inner_circle', 3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'Inner Circle Private Interview',                         'roc_inner_circle', 3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'Check Up Call',                                          'roc_inner_circle', 3, FALSE, NULL
  UNION ALL SELECT 'event_name', 'Reschedules Brand Scaling Blueprint Access',             'roc_inner_circle', 3, FALSE, NULL

  -- Priority 4: exact pipeline name
  UNION ALL SELECT 'pipeline_name', 'Brand Scaling Blueprint Booked Calls',               'roc_inner_circle', 4, FALSE, NULL
  UNION ALL SELECT 'pipeline_name', 'Dee Builds Brands MAIN Sales Pipeline',              'roc_inner_circle', 4, FALSE, NULL
  UNION ALL SELECT 'pipeline_name', 'Inner Circle 2.0 Launch',                            'roc_inner_circle', 4, FALSE, NULL
  UNION ALL SELECT 'pipeline_name', 'Inner Circle Launch',                                'roc_inner_circle', 4, FALSE, NULL
  UNION ALL SELECT 'pipeline_name', 'IC 2.0 Waitlist',                                   'roc_inner_circle', 4, FALSE, NULL
  UNION ALL SELECT 'pipeline_name', 'IC Relaunch',                                        'roc_inner_circle', 4, FALSE, NULL
  UNION ALL SELECT 'pipeline_name', 'Speed to Lead Call',                                 'roc_core',         4, FALSE, NULL

  -- Priority 5: regex utm_campaign
  UNION ALL SELECT 'regex_utm_campaign', r'inner circle|ic 2\.0|ic2\.0',                  'roc_inner_circle', 5, TRUE,  NULL
  UNION ALL SELECT 'regex_utm_campaign', r'brand scaling blueprint|bsb',                  'roc_inner_circle', 5, TRUE,  NULL
  UNION ALL SELECT 'regex_utm_campaign', r'\broc\b',                                      'roc_core',         5, TRUE,  NULL

  -- Priority 6: regex utm_source / ghl_source support signals
  UNION ALL SELECT 'regex_source', r'dee.*circle|deesinnercircle',                        'roc_inner_circle', 6, TRUE,  NULL
  UNION ALL SELECT 'regex_source', r'precisionscaling',                                   'roc_core',         6, TRUE,  NULL
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 2: Approved application form list
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_approved_application_forms` AS
SELECT form_id, form_name, canonical_offer, notes FROM (
  SELECT 'ZJ6iVYCMixldsyDi2B84' AS form_id, 'Ap2'                                                                      AS form_name, 'roc_inner_circle' AS canonical_offer, 'IC application form'                           AS notes
  UNION ALL SELECT 'qRLeE2amQM3fpctrW3Q2', 'Instagram and Age question for the Inbound Calendar on GHL ROC',            'roc_core',         'ROC inbound qualifying intake'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Step 3: Offer resolved — Calendly booking grain
-- Resolves canonical_offer for every invitee_id using the priority ladder.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.Core.dim_offer_resolved` AS
WITH bookings AS (
  SELECT
    i.invitee_id,
    i.event_name,
    i.event_type_uri,
    COALESCE(NULLIF(TRIM(et.scheduling_url), ''), '') AS scheduling_url,
    LOWER(COALESCE(NULLIF(TRIM(i.utm_campaign), ''), '')) AS utm_campaign_norm,
    LOWER(COALESCE(NULLIF(TRIM(i.utm_source), ''), ''))   AS utm_source_norm
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON i.event_type_uri = et.event_type_uri
),
candidates AS (
  SELECT
    b.invitee_id,
    r.canonical_offer,
    r.mapping_priority,
    LENGTH(r.rule_value) AS rule_value_len,
    r.rule_type,
    COUNT(*) OVER (
      PARTITION BY b.invitee_id, r.mapping_priority, LENGTH(r.rule_value)
    ) AS same_priority_same_len_count,
    ROW_NUMBER() OVER (
      PARTITION BY b.invitee_id
      ORDER BY r.mapping_priority ASC, LENGTH(r.rule_value) DESC
    ) AS resolution_rank
  FROM bookings b
  JOIN `project-41542e21-470f-4589-96d.Core.dim_offer_mapping_rules` r
    ON (
      (r.rule_type = 'booking_url'         AND NOT r.is_regex AND b.scheduling_url = r.rule_value)
      OR (r.rule_type = 'event_name'       AND NOT r.is_regex AND b.event_name = r.rule_value)
      OR (r.rule_type = 'regex_utm_campaign' AND r.is_regex AND REGEXP_CONTAINS(b.utm_campaign_norm, r.rule_value))
      OR (r.rule_type = 'regex_source'     AND r.is_regex AND REGEXP_CONTAINS(b.utm_source_norm, r.rule_value))
    )
),
top_match AS (
  SELECT * FROM candidates WHERE resolution_rank = 1
)
SELECT
  b.invitee_id,
  CASE
    WHEN t.same_priority_same_len_count > 1 THEN 'unknown_offer'
    ELSE COALESCE(t.canonical_offer, 'unknown_offer')
  END AS canonical_offer,
  CASE
    WHEN t.invitee_id IS NULL                   THEN 'unknown_offer'
    WHEN t.same_priority_same_len_count > 1     THEN 'unknown_hard_conflict'
    ELSE 'mapped_priority_rule'
  END AS match_status,
  t.rule_type,
  t.mapping_priority
FROM (SELECT DISTINCT invitee_id FROM bookings) b
LEFT JOIN top_match t ON t.invitee_id = b.invitee_id;
