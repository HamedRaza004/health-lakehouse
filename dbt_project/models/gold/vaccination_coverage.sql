{{
  config(
    materialized = 'table',
    file_format  = 'parquet'
  )
}}

with vax as (
    select
        iso_code,
        location,
        date,
        total_vaccinations,
        people_vaccinated,
        people_fully_vaccinated,
        daily_vaccinations,
        total_vaccinations_per_hundred,
        people_vaccinated_per_hundred,
        people_fully_vaccinated_per_hundred,
        _silver_processed_at
    from {{ source('silver', 'owid_vaccination_silver') }}
    where iso_code is not null
      and date is not null
),

latest_per_country as (
    select
        iso_code,
        location,
        max(date)                               as latest_date,
        max(total_vaccinations)                 as total_vaccinations,
        max(people_vaccinated)                  as people_vaccinated,
        max(people_fully_vaccinated)            as people_fully_vaccinated,
        max(people_vaccinated_per_hundred)      as pct_at_least_one_dose,
        max(people_fully_vaccinated_per_hundred) as pct_fully_vaccinated,
        max(_silver_processed_at)               as _silver_processed_at
    from vax
    group by iso_code, location
),

categorised as (
    select
        *,
        case
            when pct_fully_vaccinated >= 70 then 'high'
            when pct_fully_vaccinated >= 40 then 'medium'
            when pct_fully_vaccinated >= 10 then 'low'
            else 'very_low'
        end                                     as coverage_category,
        current_timestamp                       as _gold_created_at
    from latest_per_country
)

select * from categorised
