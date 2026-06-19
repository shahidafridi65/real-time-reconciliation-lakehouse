-- dbt Silver model: silver_products
-- Cleans and normalizes raw product records from Bronze layer.
{{ config(materialized='table') }}

WITH raw_products AS (
    SELECT * FROM {{ source('bronze', 'products') }}
),

cleaned_products AS (
    SELECT
        CAST(product_id AS VARCHAR(64))                     AS product_id,
        TRIM(product_name)                                  AS product_name,
        LOWER(TRIM(category))                               AS category,
        CAST(price AS DOUBLE PRECISION)                     AS price,
        CAST(updated_at AS TIMESTAMP)                       AS updated_at,
        CURRENT_TIMESTAMP                                   AS _transformed_at,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS _row_number
    FROM raw_products
    WHERE product_id IS NOT NULL
      AND price IS NOT NULL
      AND price > 0
)

SELECT
    product_id,
    product_name,
    category,
    price,
    updated_at,
    _transformed_at
FROM cleaned_products
WHERE _row_number = 1
