{{ config(
    materialized='view'
) }}

with cleaned as(
    select
        order_item_id,
        order_id,
        product_id,
        coalesce(cast(quantity as integer), 1) as quantity,
        coalesce(cast(price as integer), 0) as price,
        coalesce(cast(discount_amount as integer), 0) as discount_amount,
        coalesce(cast(subtotal as integer), cast(quantity as integer)*cast(price as integer)) as subtotal,
        cast(created_at as timestamp),
        cast(updated_at as timestamp)
    from {{ref('bronze_order_items')}}
    where order_item_id is not null
        and order_id is not null
        and product_id is not null
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id 
            ORDER BY updated_at DESC
        ) AS rn
    FROM cleaned
)

SELECT *
FROM ranked
WHERE rn = 1