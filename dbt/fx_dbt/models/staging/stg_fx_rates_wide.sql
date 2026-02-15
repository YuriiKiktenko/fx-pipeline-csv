{{ config(materialized='view') }}

with src as (
  select
    rate_date,
    currency,
    rate
  from {{ ref('stg_fx_rates') }}
  where currency in unnest({{ var('fx_required_currencies') }})
)

select
  rate_date,
  {% for ccy in var('fx_required_currencies') %}
  max(if(currency = '{{ ccy }}', rate, null)) as rate_{{ ccy|lower }}{% if not loop.last %},{% endif %}
  {% endfor %}
from src
group by 1
