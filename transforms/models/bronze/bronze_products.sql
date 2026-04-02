{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

select *
from {{ source('proyek_22', 'products') }}

{% if is_incremental() %}
where updated_at > (
    select max(updated_at)
    from {{ this }}
)
{% endif %}