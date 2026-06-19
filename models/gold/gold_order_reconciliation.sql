{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('silver_orders') }}
),

purchases AS (
    SELECT *
    FROM {{ ref('silver_clickstream') }}
    WHERE event_type = 'purchase'
),

server_logs AS (
    SELECT * FROM {{ ref('silver_server_logs') }}
),

matched_order_events AS (
    SELECT
        o.order_id,
        o.user_id,
        o.product_id,
        o.status AS order_status,
        o.payment_status,
        o.total_amount,
        o.placed_at,
        p.event_id,
        p.event_timestamp,
        p.amount AS clickstream_amount,
        ABS(DATEDIFF(minute, o.placed_at, p.event_timestamp)) AS event_delay_minutes,
        ROW_NUMBER() OVER (
            PARTITION BY o.order_id
            ORDER BY ABS(DATEDIFF(second, o.placed_at, p.event_timestamp)), p.event_timestamp
        ) AS match_rank
    FROM orders o
    LEFT JOIN purchases p
        ON o.user_id = p.user_id
        AND o.product_id = p.product_id
        AND p.event_timestamp BETWEEN o.placed_at - INTERVAL '24 HOURS'
                                  AND o.placed_at + INTERVAL '24 HOURS'
),

best_order_events AS (
    SELECT *
    FROM matched_order_events
    WHERE match_rank = 1
),

matched_purchase_ids AS (
    SELECT DISTINCT event_id
    FROM best_order_events
    WHERE event_id IS NOT NULL
),

operational_signals AS (
    SELECT
        o.order_id,
        COUNT(CASE WHEN sl.status_code >= 500 THEN 1 END) AS server_error_count,
        COUNT(CASE WHEN sl.status_code = 504 THEN 1 END) AS gateway_timeout_count,
        MAX(CASE WHEN sl.latency_ms >= 2000 THEN 1 ELSE 0 END) AS has_high_latency_log
    FROM orders o
    LEFT JOIN server_logs sl
        ON sl.log_timestamp BETWEEN o.placed_at - INTERVAL '5 MINUTES'
                                AND o.placed_at + INTERVAL '5 MINUTES'
    GROUP BY o.order_id
),

order_side AS (
    SELECT
        MD5('order|' || boe.order_id) AS reconciliation_id,
        'order' AS reconciliation_record_type,
        boe.order_id,
        boe.user_id,
        boe.product_id,
        boe.order_status,
        boe.payment_status,
        boe.total_amount,
        boe.placed_at,
        boe.event_id AS clickstream_event_id,
        boe.event_timestamp AS clickstream_event_timestamp,
        boe.clickstream_amount,
        boe.event_delay_minutes,
        COALESCE(os.server_error_count, 0) AS server_error_count,
        COALESCE(os.gateway_timeout_count, 0) AS gateway_timeout_count,
        COALESCE(os.has_high_latency_log, 0) = 1 AS has_high_latency_log,
        CASE WHEN boe.event_id IS NULL THEN true ELSE false END AS has_missing_clickstream,
        CASE WHEN boe.event_id IS NOT NULL AND boe.event_delay_minutes > 60 THEN true ELSE false END AS has_delayed_clickstream,
        CASE
            WHEN boe.payment_status IN ('failed', 'pending') AND boe.order_status IN ('shipped', 'delivered') THEN true
            WHEN boe.payment_status = 'paid' AND boe.order_status = 'cancelled' THEN true
            ELSE false
        END AS has_payment_mismatch,
        CASE WHEN COALESCE(os.gateway_timeout_count, 0) > 0 THEN true ELSE false END AS has_gateway_timeout,
        CASE
            WHEN boe.event_id IS NULL THEN 'missing_purchase_event'
            WHEN boe.event_delay_minutes > 60 THEN 'delayed_purchase_event'
            WHEN boe.payment_status IN ('failed', 'pending') AND boe.order_status IN ('shipped', 'delivered') THEN 'payment_not_settled'
            WHEN boe.payment_status = 'paid' AND boe.order_status = 'cancelled' THEN 'paid_cancelled_order'
            WHEN COALESCE(os.gateway_timeout_count, 0) > 0 THEN 'gateway_timeout_near_order'
            ELSE 'matched'
        END AS reconciliation_status,
        CURRENT_TIMESTAMP AS _reconciled_at
    FROM best_order_events boe
    LEFT JOIN operational_signals os ON boe.order_id = os.order_id
),

purchase_side AS (
    SELECT
        MD5('purchase|' || p.event_id) AS reconciliation_id,
        'purchase_without_order' AS reconciliation_record_type,
        CAST(NULL AS VARCHAR(64)) AS order_id,
        p.user_id,
        p.product_id,
        CAST(NULL AS VARCHAR(64)) AS order_status,
        CAST(NULL AS VARCHAR(64)) AS payment_status,
        CAST(NULL AS DOUBLE PRECISION) AS total_amount,
        CAST(NULL AS TIMESTAMP) AS placed_at,
        p.event_id AS clickstream_event_id,
        p.event_timestamp AS clickstream_event_timestamp,
        p.amount AS clickstream_amount,
        CAST(NULL AS INTEGER) AS event_delay_minutes,
        0 AS server_error_count,
        0 AS gateway_timeout_count,
        false AS has_high_latency_log,
        false AS has_missing_clickstream,
        false AS has_delayed_clickstream,
        false AS has_payment_mismatch,
        false AS has_gateway_timeout,
        'purchase_without_order' AS reconciliation_status,
        CURRENT_TIMESTAMP AS _reconciled_at
    FROM purchases p
    LEFT JOIN matched_purchase_ids mpi ON p.event_id = mpi.event_id
    WHERE mpi.event_id IS NULL
)

SELECT * FROM order_side
UNION ALL
SELECT * FROM purchase_side
