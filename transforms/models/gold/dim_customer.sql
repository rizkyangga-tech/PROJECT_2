{{ config(materialized='table') }}

SELECT
    c.customer_id,
    c.name,
    c.gender,
    c.city,
    c.birth_date,

    DATE_DIFF(CURRENT_DATE(), c.birth_date, YEAR) AS age,

    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.subtotal) AS total_spent

FROM {{ ref('silver_customers') }} as c
LEFT JOIN {{ ref('silver_orders') }} as o USING(customer_id)
LEFT JOIN {{ ref('silver_order_items') }} as oi USING(order_id)

WHERE o.status = 'completed'
GROUP BY 1,2,3,4,5