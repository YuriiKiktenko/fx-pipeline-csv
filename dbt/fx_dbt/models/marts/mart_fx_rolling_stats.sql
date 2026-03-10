{{ config(materialized='table') }}

select
  rate_date,
  currency,
  rate,
  min(rate_date) over w7   as window_start_date_7d,
  min(rate) over w7   as rate_min_7d,
  max(rate) over w7   as rate_max_7d,
  avg(rate) over w7   as rate_avg_7d,
  safe_divide(max(rate) over w7 - min(rate) over w7, min(rate) over w7)   as range_pct_7d,
  min(rate_date) over w30   as window_start_date_30d,
  min(rate) over w30  as rate_min_30d,
  max(rate) over w30  as rate_max_30d,
  avg(rate) over w30  as rate_avg_30d,
  safe_divide(max(rate) over w30 - min(rate) over w30, min(rate) over w30) as range_pct_30d
from {{ ref('stg_fx_rates') }}
window
  w7  as (partition by currency order by rate_date rows between 6 preceding and current row),
  w30 as (partition by currency order by rate_date rows between 29 preceding and current row)

order by rate_date desc, currency
