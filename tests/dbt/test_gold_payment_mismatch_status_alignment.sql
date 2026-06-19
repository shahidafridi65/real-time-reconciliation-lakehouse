SELECT *
FROM {{ ref('gold_order_reconciliation') }}
WHERE has_payment_mismatch = true
  AND reconciliation_status NOT IN ('payment_not_settled', 'paid_cancelled_order')
