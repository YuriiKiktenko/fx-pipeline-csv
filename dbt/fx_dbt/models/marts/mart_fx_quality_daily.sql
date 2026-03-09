{{ config(materialized='table') }}

{% set expected_currency_count = var('fx_required_currencies')|length - (1 if var('fx_base_currency') in var('fx_required_currencies') else 0) %}

with all_days as (
  select 
    date_val as rate_date,
    extract(dayofweek from date_val) between 2 and 6 as data_expected
  from unnest(generate_date_array(
    date("{{ var('fx_start_date') }}"),
    current_date() - 1,
    interval 1 day
  )) as date_val
),

day_stats as (
  select
    rate_date,
    count(*) as row_count,
    count(distinct currency) as currency_count
  from {{ ref('stg_fx_rates') }}
  group by 1
),

pct_change_stats as (
  select
    rate_date,
    avg(abs(pct_change)) as avg_abs_pct_change,
    max(abs(pct_change)) as max_abs_pct_change
  from {{ ref('mart_fx_pct_change') }}
  group by 1
)

select
  ad.rate_date,
  ad.data_expected,
  coalesce(ds.row_count, 0) as row_count,
  coalesce(ds.currency_count, 0) as currency_count,
  case when ad.data_expected then {{ expected_currency_count }} else 0 end as expected_currency_count,
  pcs.avg_abs_pct_change,
  pcs.max_abs_pct_change,
  case 
    when ad.data_expected and coalesce(ds.row_count, 0) = 0 then 'MISSING_DATA'
    when ad.data_expected and coalesce(ds.currency_count, 0) < case when ad.data_expected then {{ expected_currency_count }} else 0 end then 'INCOMPLETE_DATA'
    when coalesce(pcs.max_abs_pct_change, 0) > {{ var('fx_large_change_threshold', 0.01) }} then 'LARGE_CHANGE'
    else null
  end as alert
from all_days ad
left join day_stats ds on ad.rate_date = ds.rate_date
left join pct_change_stats pcs on ad.rate_date = pcs.rate_date
order by ad.rate_date desc
