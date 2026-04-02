{{ config(
    materialized='view' 
) }}

with cleaned as (
    select
        order_item_id,
        order_id,
        product_id,

        coalesce(cast(quantity as integer), 1) as quantity,
        coalesce(cast(price as integer), 0) as price,
        coalesce(cast(discount_amount as integer), 0) as discount_amount,

        coalesce(
            cast(subtotal as integer),
            coalesce(cast(quantity as integer), 1) * coalesce(cast(price as integer), 0)
        ) as subtotal,

        SAFE_CAST(created_at as timestamp) as created_at,
        SAFE_CAST(updated_at as timestamp) as updated_at,

    from {{ ref('bronze_order_items') }}
    where order_item_id is not null
        and order_id is not null
        and product_id is not null
        and created_at is not null
        and updated_at is not null
),

ranked as (
    select
        *,
        row_number() over (
            partition by order_item_id
            order by updated_at desc
        ) as rn
    from cleaned
)

select *
from ranked
where rn = 1