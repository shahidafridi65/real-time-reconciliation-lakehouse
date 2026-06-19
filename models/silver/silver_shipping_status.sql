{{ config(materialized='table') }}

WITH raw_shipping AS (
    SELECT * FROM {{ source('bronze', 'shipment_data') }}
),

transformed AS (
    SELECT
        CAST(order_id AS VARCHAR(64)) AS order_id,
        carrier,
        tracking_number,
        LOWER(status) AS status,
        CAST(status_last_updated_at AS TIMESTAMP) AS last_updated_at,
        CURRENT_TIMESTAMP AS _transformed_at,
        ROW_NUMBER() OVER (PARTITION BY tracking_number ORDER BY status_last_updated_at DESC) AS _row_number
    FROM raw_shipping
)

SELECT
    order_id,
    carrier,
    tracking_number,
    status,
    last_updated_at,
    _transformed_at
FROM transformed
WHERE _row_number = 1
