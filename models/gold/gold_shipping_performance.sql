{{ config(materialized='table') }}

SELECT *
FROM {{ ref('gold_logistics_performance') }}
