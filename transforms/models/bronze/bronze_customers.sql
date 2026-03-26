 {{ config(
    materialized='incremental',
    unique_key='customers'
) }}

select *
from {{source('proyek_22', 'customers')}}
 
    {% if is_incremental() %}
    where updated_at > coalesce(
    (select max(updated_at) from {{ this }}),
    '1900-01-01'
) - INTERVAL 1 DAY
    {% endif %}