{{ config(materialized='table') }}

SELECT
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    FORMAT_DATE('%A', date) AS weekday,
    CASE 
        WHEN FORMAT_DATE('%A', date) IN ('Saturday','Sunday') THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31')) AS date