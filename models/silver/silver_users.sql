-- dbt Silver model: silver_users
-- Cleans and normalizes raw user records from Bronze layer.
{{ config(materialized='table') }}

WITH raw_users AS (
    SELECT * FROM {{ source('bronze', 'users') }}
),

cleaned_users AS (
    SELECT
        CAST(user_id AS VARCHAR(64))                         AS user_id,
        INITCAP(TRIM(full_name))                             AS full_name,
        LOWER(TRIM(email))                                   AS email,
        UPPER(TRIM(country))                                 AS country_code,
        CAST(created_at AS TIMESTAMP)                        AS created_at,
        CURRENT_TIMESTAMP                                    AS _transformed_at,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS _row_number
    FROM raw_users
    WHERE user_id IS NOT NULL
      AND email IS NOT NULL
)

SELECT
    user_id,
    full_name,
    email,
    country_code,
    created_at,
    _transformed_at
FROM cleaned_users
WHERE _row_number = 1
