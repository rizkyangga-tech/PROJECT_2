{{ config(
    materialized='view',
) }}

with cleaned as (
    select 
        product_id,
        trim(lower(product_name)) as product_name,
        category_id,
        trim(brand) as brand,
        cast(price as integer) as price,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        row_number() over(partition by product_id order by updated_at desc) as rn
    from {{ref("bronze_products")}}
    where product_id is not null
        and product_name is not null
        and category_id is not null

)

select *
from cleaned
where rn = 1