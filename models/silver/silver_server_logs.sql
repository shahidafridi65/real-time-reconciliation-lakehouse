{{ config(materialized='table') }}

WITH raw_logs AS (
    SELECT * FROM {{ source('bronze', 'server_logs') }}
),

parsed_logs AS (
    -- If the logs were already parsed by Spark into columns
    SELECT
        CAST(timestamp AS TIMESTAMP) AS log_timestamp,
        ip,
        service,
        method,
        path,
        CAST(status_code AS INTEGER) AS status_code,
        CAST(latency_ms AS INTEGER) AS latency_ms,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM raw_logs
)

SELECT * FROM parsed_logs
