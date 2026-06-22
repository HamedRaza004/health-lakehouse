{{
  config(
    materialized = 'table',
    file_format  = 'parquet'
  )
}}

with covid_base as (
    select
        iso_code,
        location,
        continent,
        date,
        new_deaths,
        total_deaths,
        new_cases,
        total_cases,
        rolling_7d_avg_cases,
        _silver_processed_at
    from {{ source('silver', 'owid_covid_silver') }}
    where continent is not null
      and iso_code is not null
      and date is not null
),

regional_agg as (
    select
        continent                               as region,
        cast(substr(cast(date as varchar), 1, 4) as integer) as year,
        count(distinct iso_code)                as country_count,
        round(sum(new_deaths), 0)               as total_new_deaths,
        round(sum(new_cases), 0)                as total_new_cases,
        round(avg(rolling_7d_avg_cases), 2)     as avg_7d_rolling_cases,
        round(
            sum(new_deaths) / nullif(sum(new_cases), 0) * 100,
        4)                                      as case_fatality_rate_pct,
        max(_silver_processed_at)               as _silver_processed_at
    from covid_base
    group by continent,
             cast(substr(cast(date as varchar), 1, 4) as integer)
)

select
    region,
    year,
    country_count,
    total_new_deaths,
    total_new_cases,
    avg_7d_rolling_cases,
    case_fatality_rate_pct,
    _silver_processed_at,
    current_timestamp                           as _gold_created_at
from regional_agg
where region is not null
order by region, year
