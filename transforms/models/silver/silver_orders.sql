{{ config(
    materialized='view',
) }}

with cleaned as (
    select
        c.order_id,
        c.customer_id,
        case
            when c.order_date like '%/%' then to_date(replace(c.order_date, '/', '-'), 'YYYY-MM-DD')
            when c.order_date like '%-%' then to_date(c.order_date, 'YYYY-MM-DD')
        else null
        end as cast(order_date as date),
        coalesce(trim(lower(c.status)),'unknown') as status,
        coalesce(c.payment_method,'unknown') as payment_method,
        coalesce(cast(c.total_amount as integer), cast(d.subtotal as integer)) as total_amount,
        cast(c.created_at as timestamp),
        cast(c.updated_at as timestamp),
        row_number() over(partition by order_id order by c.updated_at desc) as rn
    from {{ref('bronze_orders')}} as c
    left join {{ref('silver_order_items')}} as d
    on c.order_id = d.order_id
    where order_id is not null
        and customer_id is not null

)

select *
from cleaned
where rn = 1
