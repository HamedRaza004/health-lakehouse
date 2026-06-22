{{
  config(
    materialized = 'table',
    file_format  = 'parquet'
  )
}}

with cdc_base as (
    select
        condition,
        reporting_area,
        year,
        week,
        current_week,
        previous_52_week_max,
        rolling_avg_cases,
        is_outbreak,
        _silver_processed_at
    from {{ source('silver', 'cdc_nndss_silver') }}
    where condition is not null
      and reporting_area is not null
      and current_week is not null
),

with_severity as (
    select
        *,
        case
            when current_week > previous_52_week_max * 2.0 then 'critical'
            when current_week > previous_52_week_max * 1.5 then 'high'
            when is_outbreak = true               then 'medium'
            else 'watch'
        end                                     as severity,
        round(
            case
                when rolling_avg_cases > 0
                then current_week / rolling_avg_cases
                else null
            end,
        2)                                      as cases_vs_rolling_avg_ratio
    from cdc_base
    where is_outbreak = true
       or current_week > previous_52_week_max * 1.5
)

select
    condition,
    reporting_area,
    year,
    week,
    current_week                                as current_week_cases,
    previous_52_week_max,
    rolling_avg_cases,
    cases_vs_rolling_avg_ratio,
    severity,
    is_outbreak,
    _silver_processed_at,
    current_timestamp                           as _gold_created_at
from with_severity
order by severity, cases_vs_rolling_avg_ratio desc
