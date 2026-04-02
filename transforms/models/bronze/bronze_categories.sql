{{ config(
    materialized='table'
) }}

with source_data as (
    select *
    from {{ source('proyek_22', 'categories') }}
)
select *    
from source_data