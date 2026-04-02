{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

select *
from {{ source('proyek_22', 'order_items') }}

{% if is_incremental() %}
where updated_at > (
    select max(updated_at)
    from {{ this }}
)
{% endif %}