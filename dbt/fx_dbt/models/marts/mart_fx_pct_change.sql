{{ config(materialized='table') }}

with with_prev_rate as (
  select
    rate_date,
    currency,
    rate,
    lag(rate) over (
      partition by currency 
      order by rate_date
    ) as prev_rate
  from {{ ref('stg_fx_rates') }}
)

select
  rate_date,
  currency,
  rate,
  prev_rate,
  safe_divide(rate, prev_rate) - 1 as pct_change
from with_prev_rate
where prev_rate is not null
order by currency, rate_date desc
