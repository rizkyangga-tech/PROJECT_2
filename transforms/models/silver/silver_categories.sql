{{ config(materialized='view') }}
select
    category_id,
    trim(category_name) as category_name
from {{ref('bronze_categories')}}
where category_id is not null 
    and category_name is not null
