{{ config(
    materialized='view'
) }}

{{ config(materialized='view', unique_key='customer_id') }}
with ranked as(
    select
        customer_id,
        lower(trim(gender)) as gender,
        trim(name) as name,
        case
            when birth_date like '%/%' then to_date(replace(birth_date, '/', '-'), 'YYYY-MM-DD')
            when birth_date like '%-%' then to_date(birth_date, 'YYYY-MM-DD')
            else '-'
        end as birth_date,
        trim(city) as city,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        row_number() over(partition by customer_id order by updated_at desc nulls last) as rn
    from {{ref('bronze_customers')}}
    where customer_id is not null
        and created_at is not null
        and updated_at is not null
)

select
    customer_id,
    case 
        when name is null then 'unknown'
        else name
    end as name,
    case
        when gender in ('m', 'male', 'l', 'cowok', 'laki', 'laki-laki') then 'male'
        when gender in ('f', 'female', 'p', 'perempuan') then 'female'
        else 'unknown'
    end as gender,
    coalesce(birth_date, '-') as birth_date,
    coalesce(city, '-') as city
    created_at,
    updated_at
from ranked
where rn = 1
