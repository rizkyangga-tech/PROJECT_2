 {{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

select *
from {{source('proyek_22', 'customers')}}
 
    {% if is_incremental() %}
    where updated_at > coalesce(
    (select max(updated_at) from {{ this }}),
    '1900-01-01'
)
    {% endif %}