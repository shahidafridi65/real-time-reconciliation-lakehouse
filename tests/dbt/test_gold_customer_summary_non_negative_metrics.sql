SELECT *
FROM {{ ref('gold_customer_summary') }}
WHERE lifetime_spend < 0
   OR total_orders < 0
   OR avg_order_value < 0
   OR total_clickstream_events < 0
   OR purchase_events < 0
   OR failed_payment_orders < 0
