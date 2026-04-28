-- STG: transaction-level flattening from raw JSON payload
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_fanbasis_transactions` AS
SELECT
  transaction_id,
  event_ts,
  ingested_at,
  source,
  endpoint,
  backfill_run_id,
  is_backfill,
  payload_json,
  JSON_VALUE(payload_json, '$.id') AS txn_id_raw,
  JSON_VALUE(payload_json, '$.transaction_date') AS transaction_date_raw,
  SAFE_CAST(JSON_VALUE(payload_json, '$.amount') AS NUMERIC) AS amount,
  SAFE_CAST(JSON_VALUE(payload_json, '$.fee_amount') AS NUMERIC) AS fee_amount,
  SAFE_CAST(JSON_VALUE(payload_json, '$.net_amount') AS NUMERIC) AS net_amount,
  JSON_VALUE(payload_json, '$.fan.id') AS customer_id,
  JSON_VALUE(payload_json, '$.fan.name') AS customer_name,
  JSON_VALUE(payload_json, '$.fan.email') AS customer_email,
  JSON_VALUE(payload_json, '$.fan.phone') AS customer_phone,
  JSON_VALUE(payload_json, '$.fan.country_code') AS customer_country_code,
  JSON_VALUE(payload_json, '$.product.id') AS product_id,
  JSON_VALUE(payload_json, '$.product.title') AS product_title,
  JSON_VALUE(payload_json, '$.product.internal_name') AS product_internal_name,
  JSON_VALUE(payload_json, '$.product.description') AS product_description,
  SAFE_CAST(JSON_VALUE(payload_json, '$.product.price') AS NUMERIC) AS product_price,
  JSON_VALUE(payload_json, '$.product.payment_link') AS product_payment_link,
  JSON_VALUE(payload_json, '$.service.id') AS service_id,
  JSON_VALUE(payload_json, '$.service.title') AS service_title,
  JSON_VALUE(payload_json, '$.service.internal_name') AS service_internal_name,
  JSON_VALUE(payload_json, '$.service.description') AS service_description,
  SAFE_CAST(JSON_VALUE(payload_json, '$.service.price') AS NUMERIC) AS service_price,
  JSON_VALUE(payload_json, '$.service.payment_link') AS service_payment_link,
  JSON_VALUE(payload_json, '$.servicePayment.id') AS service_payment_id,
  JSON_VALUE(payload_json, '$.servicePayment.payment_type') AS payment_type,
  SAFE_CAST(JSON_VALUE(payload_json, '$.servicePayment.fund_release_on') AS TIMESTAMP) AS fund_release_on,
  SAFE_CAST(JSON_VALUE(payload_json, '$.servicePayment.fund_released') AS BOOL) AS fund_released,
  JSON_QUERY_ARRAY(payload_json, '$.refunds') AS refunds_array
FROM `project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`;

-- STG: explode refunds array
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_fanbasis_refunds` AS
SELECT
  t.transaction_id,
  t.event_ts AS transaction_event_ts,
  t.customer_id,
  t.product_id,
  JSON_VALUE(r, '$.id') AS refund_id,
  JSON_VALUE(r, '$.payment_id') AS refund_payment_id,
  SAFE_CAST(JSON_VALUE(r, '$.created_at') AS TIMESTAMP) AS refund_created_at,
  SAFE_CAST(JSON_VALUE(r, '$.amount') AS NUMERIC) AS refund_amount,
  SAFE_CAST(JSON_VALUE(r, '$.amount_gross') AS NUMERIC) AS refund_amount_gross,
  SAFE_CAST(JSON_VALUE(r, '$.fee') AS NUMERIC) AS refund_fee,
  SAFE_CAST(JSON_VALUE(r, '$.refund_cost') AS NUMERIC) AS refund_cost
FROM `project-41542e21-470f-4589-96d.STG.stg_fanbasis_transactions` t,
UNNEST(IFNULL(t.refunds_array, [])) AS r;

-- CORE FACT: canonical transactions
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fanbasis_transactions` AS
SELECT
  transaction_id,
  event_ts,
  amount,
  fee_amount,
  net_amount,
  customer_id,
  product_id,
  service_id,
  service_payment_id,
  payment_type,
  fund_release_on,
  fund_released,
  source,
  endpoint,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_fanbasis_transactions`;

-- CORE DIM: customers
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_fanbasis_customers` AS
SELECT
  customer_id,
  ANY_VALUE(customer_name) AS name,
  ANY_VALUE(customer_email) AS email,
  ANY_VALUE(customer_phone) AS phone,
  ANY_VALUE(customer_country_code) AS country_code,
  MIN(event_ts) AS first_seen_ts,
  MAX(event_ts) AS last_seen_ts,
  SUM(IFNULL(net_amount, 0)) AS lifetime_net_amount,
  COUNT(DISTINCT transaction_id) AS lifetime_transaction_count
FROM `project-41542e21-470f-4589-96d.STG.stg_fanbasis_transactions`
WHERE customer_id IS NOT NULL
GROUP BY customer_id;

-- CORE DIM: products
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_fanbasis_products` AS
SELECT
  product_id,
  ANY_VALUE(product_title) AS title,
  ANY_VALUE(product_internal_name) AS internal_name,
  ANY_VALUE(product_description) AS description,
  ANY_VALUE(product_price) AS price,
  ANY_VALUE(product_payment_link) AS payment_link,
  MIN(event_ts) AS first_seen_ts,
  MAX(event_ts) AS last_seen_ts,
  COUNT(DISTINCT transaction_id) AS lifetime_transaction_count,
  SUM(IFNULL(net_amount, 0)) AS lifetime_net_amount
FROM `project-41542e21-470f-4589-96d.STG.stg_fanbasis_transactions`
WHERE product_id IS NOT NULL
GROUP BY product_id;

-- CORE FACT: refunds
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fanbasis_refunds` AS
SELECT
  refund_id,
  transaction_id,
  refund_payment_id,
  transaction_event_ts,
  refund_created_at,
  customer_id,
  product_id,
  refund_amount,
  refund_amount_gross,
  refund_fee,
  refund_cost
FROM `project-41542e21-470f-4589-96d.STG.stg_fanbasis_refunds`;

-- CORE FACT: payout status by service payment
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fanbasis_payout_status` AS
SELECT
  transaction_id,
  service_payment_id,
  payment_type,
  fund_release_on,
  fund_released,
  event_ts,
  ingested_at
FROM `project-41542e21-470f-4589-96d.STG.stg_fanbasis_transactions`
WHERE service_payment_id IS NOT NULL;
