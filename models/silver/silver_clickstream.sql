{{ config(materialized='table') }}

WITH raw_events AS (
    SELECT * FROM {{ source('bronze', 'clickstream_events') }}
),

transformed AS (
    SELECT
        CAST(event_id AS VARCHAR(128)) AS event_id,
        event_type,
        CAST(user_id AS VARCHAR(64)) AS user_id,
        CAST(event_time AS TIMESTAMP) AS event_timestamp,
        CAST(product_id AS VARCHAR(64)) AS product_id,
        category,
        device_type,
        country,
        CAST(price AS DOUBLE PRECISION) AS amount,
        CAST(quantity AS INTEGER) AS quantity,
        CURRENT_TIMESTAMP AS _transformed_at,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time DESC) AS _row_number
    FROM raw_events
)

SELECT
    event_id,
    event_type,
    user_id,
    event_timestamp,
    product_id,
    category,
    device_type,
    country,
    amount,
    quantity,
    _transformed_at
FROM transformed
WHERE _row_number = 1
