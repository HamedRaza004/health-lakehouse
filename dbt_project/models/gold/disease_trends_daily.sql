{{
  config(
    materialized = 'table',
    file_format  = 'parquet'
  )
}}

with who_base as (
    select
        indicator_code,
        indicator_name,
        country_code,
        country_name,
        year,
        sex,
        value                                   as metric_value,
        low                                     as metric_low,
        high                                    as metric_high,
        _silver_processed_at
    from {{ source('silver', 'who_gho_silver') }}
    where value is not null
      and country_code is not null
      and year is not null
),

ranked as (
    select
        *,
        avg(metric_value) over (
            partition by indicator_code, country_code, sex
            order by year
            rows between 2 preceding and current row
        ) as rolling_3yr_avg
    from who_base
)

select
    indicator_code,
    indicator_name,
    country_code,
    country_name,
    year,
    sex,
    metric_value,
    metric_low,
    metric_high,
    round(rolling_3yr_avg, 4)                   as rolling_3yr_avg,
    round(metric_value - rolling_3yr_avg, 4)    as deviation_from_avg,
    _silver_processed_at,
    current_timestamp                           as _gold_created_at
from ranked