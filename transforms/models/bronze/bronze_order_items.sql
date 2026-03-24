{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

SELECT *
FROM {{ source('proyek_22', 'order_items') }}

    {% if is_incremental() %}
        WHERE updated_at > (
            SELECT MAX(updated_at) FROM {{ this }}
        )
    {% endif %}