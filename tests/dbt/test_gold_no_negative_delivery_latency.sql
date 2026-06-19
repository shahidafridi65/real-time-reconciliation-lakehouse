SELECT *
FROM {{ ref('gold_logistics_performance') }}
WHERE delivery_latency_hours < 0
