{{ config(materialized='table') }}

select
    c.order_item_id,
    c.order_id,
    d.customer_id,
    c.product_id,
    d.order_date,
    d.status,
    d.payment_method,
    c.quantity,
    c.price,
    c.discount_amount,
    c.subtotal
    (c.price * c.quantity) as gross_profit,
    (cprice * c.quantity - cdiscount_amount) as net_value

FROM {{ ref('silver_order_items') }} as c
JOIN {{ ref('silver_orders') }} as d
    ON c.order_id = d.order_id

WHERE d.status = 'completed'
