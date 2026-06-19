{{ config(materialized='table') }}

WITH users AS (
    SELECT * FROM {{ ref('silver_users') }}
),

orders AS (
    SELECT * FROM {{ ref('silver_orders') }}
),

clickstream AS (
    SELECT * FROM {{ ref('silver_clickstream') }}
),

products AS (
    SELECT * FROM {{ ref('silver_products') }}
),

order_metrics AS (
    SELECT
        user_id,
        COALESCE(SUM(total_amount), 0) AS lifetime_spend,
        COUNT(order_id) AS total_orders,
        COALESCE(AVG(total_amount), 0) AS avg_order_value,
        MAX(placed_at) AS last_order_date,
        SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) AS failed_payment_orders
    FROM orders
    GROUP BY user_id
),

category_rank AS (
    SELECT
        o.user_id,
        p.category,
        COUNT(*) AS category_orders,
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id
            ORDER BY COUNT(*) DESC, p.category
        ) AS category_rank
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    GROUP BY o.user_id, p.category
),

activity_rank AS (
    SELECT
        user_id,
        event_type,
        event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY event_timestamp DESC
        ) AS activity_rank
    FROM clickstream
),

activity_metrics AS (
    SELECT
        user_id,
        MAX(event_timestamp) AS last_activity_date,
        COUNT(*) AS total_clickstream_events,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_events
    FROM clickstream
    GROUP BY user_id
)

SELECT
    u.user_id,
    u.full_name,
    u.email,
    u.country_code,
    u.created_at,
    COALESCE(om.lifetime_spend, 0) AS lifetime_spend,
    COALESCE(om.total_orders, 0) AS total_orders,
    COALESCE(om.avg_order_value, 0) AS avg_order_value,
    cr.category AS favorite_category,
    om.last_order_date,
    CASE
        WHEN om.last_order_date IS NULL THEN NULL
        ELSE DATEDIFF(day, om.last_order_date, CURRENT_TIMESTAMP)
    END AS days_since_last_order,
    am.last_activity_date,
    ar.event_type AS most_recent_event_type,
    COALESCE(am.total_clickstream_events, 0) AS total_clickstream_events,
    COALESCE(am.purchase_events, 0) AS purchase_events,
    COALESCE(om.failed_payment_orders, 0) AS failed_payment_orders,
    CASE
        WHEN COALESCE(om.lifetime_spend, 0) >= 1000 THEN 'high_value'
        WHEN COALESCE(om.lifetime_spend, 0) >= 500 THEN 'medium_value'
        WHEN COALESCE(om.lifetime_spend, 0) > 0 THEN 'low_value'
        ELSE 'new'
    END AS customer_segment,
    CASE
        WHEN am.last_activity_date IS NULL THEN 'new'
        WHEN DATEDIFF(day, am.last_activity_date, CURRENT_TIMESTAMP) <= 7 THEN 'active'
        WHEN DATEDIFF(day, am.last_activity_date, CURRENT_TIMESTAMP) <= 30 THEN 'at_risk'
        ELSE 'churned'
    END AS activity_status,
    CURRENT_TIMESTAMP AS _profile_updated_at
FROM users u
LEFT JOIN order_metrics om ON u.user_id = om.user_id
LEFT JOIN category_rank cr ON u.user_id = cr.user_id AND cr.category_rank = 1
LEFT JOIN activity_metrics am ON u.user_id = am.user_id
LEFT JOIN activity_rank ar ON u.user_id = ar.user_id AND ar.activity_rank = 1
