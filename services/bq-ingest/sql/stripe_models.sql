-- STG: Charges
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_charges` AS
SELECT
  object_id AS charge_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.customer') AS customer_id,
  JSON_VALUE(payload_json, '$.invoice') AS invoice_id,
  JSON_VALUE(payload_json, '$.payment_intent') AS payment_intent_id,
  JSON_VALUE(payload_json, '$.currency') AS currency,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount') AS NUMERIC) / 100 AS amount,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount_captured') AS NUMERIC) / 100 AS amount_captured,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount_refunded') AS NUMERIC) / 100 AS amount_refunded,
  JSON_VALUE(payload_json, '$.status') AS status,
  SAFE_CAST(JSON_VALUE(payload_json, '$.paid') AS BOOL) AS paid,
  JSON_VALUE(payload_json, '$.description') AS description,
  JSON_VALUE(payload_json, '$.billing_details.email') AS billing_email,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'charges';

-- STG: Refunds
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_refunds` AS
SELECT
  object_id AS refund_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.charge') AS charge_id,
  JSON_VALUE(payload_json, '$.payment_intent') AS payment_intent_id,
  JSON_VALUE(payload_json, '$.currency') AS currency,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount') AS NUMERIC) / 100 AS amount,
  JSON_VALUE(payload_json, '$.reason') AS reason,
  JSON_VALUE(payload_json, '$.status') AS status,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'refunds';

-- STG: Customers
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_customers` AS
SELECT
  object_id AS customer_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.email') AS email,
  JSON_VALUE(payload_json, '$.name') AS name,
  JSON_VALUE(payload_json, '$.phone') AS phone,
  JSON_VALUE(payload_json, '$.address.country') AS country,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'customers';

-- STG: Products
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_products` AS
SELECT
  object_id AS product_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.name') AS name,
  JSON_VALUE(payload_json, '$.description') AS description,
  SAFE_CAST(JSON_VALUE(payload_json, '$.active') AS BOOL) AS active,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'products';

-- STG: Prices
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_prices` AS
SELECT
  object_id AS price_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.product') AS product_id,
  JSON_VALUE(payload_json, '$.currency') AS currency,
  SAFE_CAST(JSON_VALUE(payload_json, '$.unit_amount') AS NUMERIC) / 100 AS unit_amount,
  JSON_VALUE(payload_json, '$.recurring.interval') AS recurring_interval,
  JSON_VALUE(payload_json, '$.type') AS price_type,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'prices';

-- STG: Subscriptions
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_subscriptions` AS
SELECT
  object_id AS subscription_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.customer') AS customer_id,
  JSON_VALUE(payload_json, '$.status') AS status,
  SAFE_CAST(JSON_VALUE(payload_json, '$.cancel_at_period_end') AS BOOL) AS cancel_at_period_end,
  SAFE_CAST(JSON_VALUE(payload_json, '$.current_period_start') AS INT64) AS current_period_start_unix,
  SAFE_CAST(JSON_VALUE(payload_json, '$.current_period_end') AS INT64) AS current_period_end_unix,
  TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(payload_json, '$.current_period_start') AS INT64)) AS current_period_start_ts,
  TIMESTAMP_SECONDS(SAFE_CAST(JSON_VALUE(payload_json, '$.current_period_end') AS INT64)) AS current_period_end_ts,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'subscriptions';

-- STG: Invoices
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_invoices` AS
SELECT
  object_id AS invoice_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.customer') AS customer_id,
  JSON_VALUE(payload_json, '$.subscription') AS subscription_id,
  JSON_VALUE(payload_json, '$.currency') AS currency,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount_due') AS NUMERIC) / 100 AS amount_due,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount_paid') AS NUMERIC) / 100 AS amount_paid,
  JSON_VALUE(payload_json, '$.status') AS status,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'invoices';

-- STG: Disputes
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_stripe_disputes` AS
SELECT
  object_id AS dispute_id,
  event_ts,
  ingested_at,
  backfill_run_id,
  JSON_VALUE(payload_json, '$.charge') AS charge_id,
  JSON_VALUE(payload_json, '$.payment_intent') AS payment_intent_id,
  JSON_VALUE(payload_json, '$.currency') AS currency,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount') AS NUMERIC) / 100 AS amount,
  JSON_VALUE(payload_json, '$.reason') AS reason,
  JSON_VALUE(payload_json, '$.status') AS status,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
WHERE object_type = 'disputes';

-- CORE: Payment fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_stripe_payments` AS
SELECT
  charge_id AS payment_id,
  event_ts,
  customer_id,
  invoice_id,
  payment_intent_id,
  currency,
  amount,
  amount_captured,
  amount_refunded,
  status,
  paid,
  description,
  billing_email,
  ingested_at,
  backfill_run_id,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_charges`;

-- CORE: Customer dim
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_stripe_customers` AS
SELECT
  customer_id,
  ANY_VALUE(name) AS name,
  ANY_VALUE(email) AS email,
  ANY_VALUE(phone) AS phone,
  ANY_VALUE(country) AS country,
  MIN(event_ts) AS first_seen_ts,
  MAX(event_ts) AS last_seen_ts
FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_customers`
GROUP BY customer_id;

-- CORE: Product dim joined with latest known price
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_stripe_products` AS
WITH latest_price AS (
  SELECT
    product_id,
    ANY_VALUE(price_id) AS price_id,
    ANY_VALUE(currency) AS currency,
    ANY_VALUE(unit_amount) AS unit_amount,
    ANY_VALUE(recurring_interval) AS recurring_interval,
    ANY_VALUE(price_type) AS price_type
  FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_prices`
  GROUP BY product_id
)
SELECT
  p.product_id,
  ANY_VALUE(p.name) AS name,
  ANY_VALUE(p.description) AS description,
  ANY_VALUE(p.active) AS active,
  lp.price_id,
  lp.currency,
  lp.unit_amount,
  lp.recurring_interval,
  lp.price_type,
  MIN(p.event_ts) AS first_seen_ts,
  MAX(p.event_ts) AS last_seen_ts
FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_products` p
LEFT JOIN latest_price lp
  ON lp.product_id = p.product_id
GROUP BY p.product_id, lp.price_id, lp.currency, lp.unit_amount, lp.recurring_interval, lp.price_type;

-- CORE: Refund fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_stripe_refunds` AS
SELECT
  refund_id,
  charge_id,
  payment_intent_id,
  event_ts,
  currency,
  amount,
  reason,
  status,
  ingested_at,
  backfill_run_id,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_refunds`;

-- CORE: Subscription fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_stripe_subscriptions` AS
SELECT
  subscription_id,
  customer_id,
  status,
  cancel_at_period_end,
  current_period_start_ts,
  current_period_end_ts,
  event_ts,
  ingested_at,
  backfill_run_id,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_subscriptions`;

-- CORE: Invoice fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_stripe_invoices` AS
SELECT
  invoice_id,
  customer_id,
  subscription_id,
  event_ts,
  currency,
  amount_due,
  amount_paid,
  status,
  ingested_at,
  backfill_run_id,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_invoices`;

-- CORE: Dispute fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_stripe_disputes` AS
SELECT
  dispute_id,
  charge_id,
  payment_intent_id,
  event_ts,
  currency,
  amount,
  reason,
  status,
  ingested_at,
  backfill_run_id,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_stripe_disputes`;
