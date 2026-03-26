{{ config(materialized='table') }}

SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.price,
    c.category_id,
    c.category_name

FROM {{ ref('silver_products') }} p
LEFT JOIN {{ ref('silver_categories') }} c
    ON p.category_id = c.category_id