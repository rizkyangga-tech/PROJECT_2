{{ config(
    materialized='view'
) }}

with order_items_agg as (
    select
        order_id,
        sum(subtotal) as total_amount
    from {{ ref('silver_order_items') }}
    group by order_id
),

cleaned as (
    select
        c.order_id,
        c.customer_id,

        SAFE_CAST(c.order_date as date) as order_date,

        coalesce(trim(lower(c.status)), 'unknown') as status,
        coalesce(c.payment_method, 'unknown') as payment_method,

        coalesce(
            cast(c.total_amount as numeric),
            cast(o.total_amount as numeric)
        ) as total_amount,

        SAFE_CAST(c.created_at as timestamp) as created_at,
        SAFE_CAST(c.updated_at as timestamp) as updated_at,

        row_number() over (
            partition by c.order_id
            order by c.updated_at desc
        ) as rn

    from {{ ref('bronze_orders') }} c
    left join order_items_agg o
        on c.order_id = o.order_id

    where c.order_id is not null
      and c.customer_id is not null
)

select
    order_id,
    customer_id,
    order_date,
    status,
    payment_method,
    total_amount,
    created_at,
    updated_at
from cleaned
where rn = 1