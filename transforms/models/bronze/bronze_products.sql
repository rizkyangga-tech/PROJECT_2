{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

SELECT *
FROM {{ source('proyek_22', 'products') }}

    {% if is_incremental() %}
        WHERE updated_at > (
            SELECT MAX(updated_at) FROM {{ this }}
        ) - INTERVAL 1 DAY
    {% endif %}