-- Identity bridge coverage + gap analysis (run after marts refresh).

-- 1) Coverage headline by customer and revenue.
WITH revenue_by_customer AS (
  SELECT
    customer_id,
    SUM(SAFE_CAST(net_amount AS NUMERIC)) AS net_amount
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
  GROUP BY customer_id
)
SELECT
  b.bridge_status,
  COUNT(DISTINCT b.customer_id) AS customers,
  ROUND(SUM(IFNULL(r.net_amount, 0)), 2) AS net_amount
FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment` b
LEFT JOIN revenue_by_customer r
  USING (customer_id)
GROUP BY b.bridge_status
ORDER BY net_amount DESC;

-- 2) Match-method mix by revenue.
WITH revenue_by_customer AS (
  SELECT
    customer_id,
    SUM(SAFE_CAST(net_amount AS NUMERIC)) AS net_amount
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
  GROUP BY customer_id
)
SELECT
  b.match_method,
  b.bridge_status,
  COUNT(DISTINCT b.customer_id) AS customers,
  ROUND(SUM(IFNULL(r.net_amount, 0)), 2) AS net_amount
FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment` b
LEFT JOIN revenue_by_customer r
  USING (customer_id)
GROUP BY b.match_method, b.bridge_status
ORDER BY net_amount DESC;

-- 3) Root-cause buckets for unmatched customers.
WITH no_candidate AS (
  SELECT
    customer_id,
    customer_email_canon,
    customer_phone_last10
  FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment`
  WHERE bridge_status = 'no_candidate'
),
revenue_by_customer AS (
  SELECT
    customer_id,
    SUM(SAFE_CAST(net_amount AS NUMERIC)) AS net_amount
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
  GROUP BY customer_id
)
SELECT
  CASE
    WHEN customer_email_canon IS NULL AND customer_phone_last10 IS NULL THEN 'missing_email_and_phone'
    WHEN customer_email_canon IS NULL THEN 'missing_email'
    WHEN customer_phone_last10 IS NULL THEN 'missing_phone'
    ELSE 'has_email_and_phone_no_match'
  END AS root_cause_bucket,
  COUNT(*) AS customers,
  ROUND(SUM(IFNULL(r.net_amount, 0)), 2) AS net_amount
FROM no_candidate n
LEFT JOIN revenue_by_customer r
  USING (customer_id)
GROUP BY root_cause_bucket
ORDER BY net_amount DESC, customers DESC;

-- 4) Highest-revenue unmatched customers for manual review.
WITH no_candidate AS (
  SELECT
    customer_id,
    customer_email,
    customer_phone,
    customer_email_canon
  FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment`
  WHERE bridge_status = 'no_candidate'
),
revenue_by_customer AS (
  SELECT
    customer_id,
    ROUND(SUM(SAFE_CAST(net_amount AS NUMERIC)), 2) AS net_amount
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
  GROUP BY customer_id
)
SELECT
  n.customer_id,
  n.customer_email,
  n.customer_phone,
  n.customer_email_canon,
  r.net_amount
FROM no_candidate n
LEFT JOIN revenue_by_customer r
  USING (customer_id)
ORDER BY r.net_amount DESC, n.customer_id
LIMIT 50;
