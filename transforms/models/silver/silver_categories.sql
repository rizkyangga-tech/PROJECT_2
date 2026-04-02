{{ config(materialized='view') }}

with ranked as (
    select
        category_id,
        trim(category_name) as category_name,

        row_number() over (
            partition by category_id
            order by category_name
        ) as rn

    from {{ ref('bronze_categories') }}
    where category_id is not null 
        and category_name is not null
)

select
    category_id,
    category_name
from ranked
where rn = 1