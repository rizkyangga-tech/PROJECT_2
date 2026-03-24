{{ config(
    materialized='incremental',
    unique_key='categories_id'
) }}
select *
from {{source('proyek_22', 'categories')}}