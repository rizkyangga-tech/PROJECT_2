{{ config(materialized='table') }}

SELECT
    customer_id,
    name,
    gender,
    city,
    birth_date
FROM {{ ref('silver_customers') }}