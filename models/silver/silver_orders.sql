-- This is a dbt model. It transforms raw Bronze orders into a clean Silver table.
{{ config(materialized='table') }}

WITH raw_orders AS (
    SELECT * FROM {{ source('bronze', 'orders') }}
),

cleaned_orders AS (
    SELECT
        CAST(order_id AS VARCHAR(64)) AS order_id,
        CAST(user_id AS VARCHAR(64)) AS user_id,
        CAST(product_id AS VARCHAR(64)) AS product_id,
        CAST(quantity AS INTEGER) AS quantity,
        LOWER(order_status) AS status,
        LOWER(payment_status) AS payment_status,
        CAST(total_amount AS DOUBLE PRECISION) AS total_amount,
        CAST(order_placed_at AS TIMESTAMP) AS placed_at,
        CURRENT_TIMESTAMP AS _transformed_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_placed_at DESC) AS _row_number
    FROM raw_orders
)

SELECT
    order_id,
    user_id,
    product_id,
    quantity,
    status,
    payment_status,
    total_amount,
    placed_at,
    _transformed_at
FROM cleaned_orders
WHERE _row_number = 1
