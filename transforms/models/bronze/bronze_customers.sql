{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

select *
from {{ source('proyek_22', 'customers') }}

{% if is_incremental() %}
where updated_at > (
    select max(updated_at)
    from {{ this }}
)
{% endif %}