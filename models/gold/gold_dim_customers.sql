{{ config(materialized='table') }}

SELECT *
FROM {{ ref('gold_customer_summary') }}
