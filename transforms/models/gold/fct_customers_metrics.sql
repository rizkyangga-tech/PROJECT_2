{{ config(materialized='table') }}

SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(net_value) AS total_spent,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM {{ ref('fact_sales') }}
GROUP BY customer_id