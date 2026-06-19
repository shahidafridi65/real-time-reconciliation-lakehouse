{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('silver_orders') }}
),

shipping AS (
    SELECT * FROM {{ ref('silver_shipping_status') }}
),

order_shipping AS (
    SELECT
        o.order_id,
        o.user_id,
        o.placed_at,
        o.status AS order_status,
        o.total_amount,
        s.carrier,
        s.tracking_number,
        s.status AS shipping_status,
        s.last_updated_at AS shipping_last_updated,
        DATEDIFF(hour, o.placed_at, s.last_updated_at) AS delivery_latency_hours
    FROM orders o
    LEFT JOIN shipping s ON o.order_id = s.order_id
),

classified AS (
    SELECT
        *,
        CASE WHEN tracking_number IS NULL THEN true ELSE false END AS missing_shipment,
        CASE WHEN shipping_status = 'delayed' THEN true ELSE false END AS carrier_reported_delay,
        CASE WHEN delivery_latency_hours > 72 THEN true ELSE false END AS sla_breach,
        CASE
            WHEN tracking_number IS NULL THEN 'missing'
            WHEN delivery_latency_hours <= 24 THEN 'fast'
            WHEN delivery_latency_hours <= 48 THEN 'standard'
            WHEN delivery_latency_hours <= 72 THEN 'slow'
            ELSE 'breached'
        END AS delivery_tier,
        CASE
            WHEN tracking_number IS NULL THEN 'missing_shipment'
            WHEN shipping_status = 'delayed' THEN 'carrier_delay'
            WHEN delivery_latency_hours > 72 THEN 'sla_breach'
            WHEN shipping_status = 'delivered' THEN 'delivered'
            ELSE 'in_progress'
        END AS logistics_status
    FROM order_shipping
)

SELECT
    MD5('logistics|' || order_id) AS logistics_id,
    order_id,
    user_id,
    placed_at,
    order_status,
    total_amount,
    carrier,
    tracking_number,
    shipping_status,
    shipping_last_updated,
    delivery_latency_hours,
    missing_shipment,
    carrier_reported_delay,
    sla_breach,
    delivery_tier,
    logistics_status,
    CURRENT_TIMESTAMP AS _analyzed_at
FROM classified
