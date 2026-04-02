{{ config(
    materialized='view'
) }}

with cleaned as (
    select 
        product_id,

        coalesce(nullif(trim(lower(product_name)), ''), 'unknown') as product_name,

        category_id,

        coalesce(nullif(trim(lower(brand)), ''), 'unknown') as brand,

        coalesce(cast(price as numeric), 0) as price,

        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,

        row_number() over(
            partition by product_id 
            order by updated_at desc nulls last
        ) as rn

    from {{ ref("bronze_products") }}
    where product_id is not null
        and product_name is not null
        and category_id is not null
        and created_at is not null
        and updated_at is not null
)

select
    product_id,
    product_name,
    category_id,
    brand,
    price,
    created_at,
    updated_at
from cleaned
where rn = 1