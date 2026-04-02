{{ config(materialized='view') }}

with ranked as (
    select
        customer_id,
        lower(trim(gender)) as gender,
        coalesce(nullif(trim(name), ''), 'unknown') as name,

        case
            when birth_date like '%/%' then SAFE.PARSE_DATE('%Y-%m-%d', replace(trim(birth_date), '/', '-'))
            when birth_date like '%-%' then SAFE.PARSE_DATE('%Y-%m-%d', trim(birth_date))
            else null
        end as birth_date,

        coalesce(nullif(trim(city), ''), '-') as city,

        SAFE_CAST(created_at as timestamp) as created_at,
        SAFE_CAST(updated_at as timestamp) as updated_at,

        row_number() over (
            partition by customer_id 
            order by updated_at desc nulls last
        ) as rn

    from {{ ref('bronze_customers') }}
    where customer_id is not null
)

select
    customer_id,
    name,
    case
        when gender in ('m', 'male', 'l', 'cowok', 'laki', 'laki-laki') then 'male'
        when gender in ('f', 'female', 'p', 'perempuan') then 'female'
        else 'unknown'
    end as gender,
    birth_date,
    city,
    created_at,
    updated_at
from ranked
where rn = 1
