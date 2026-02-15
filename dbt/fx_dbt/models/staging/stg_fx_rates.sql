{{ config(materialized='table') }}

select
  rate_date,
  date_trunc(rate_date, month) as rate_month,
  "{{ var('fx_base_currency', 'EUR') }}" as base_currency,
  upper(trim(currency)) as currency,
  rate
from {{ source('fx_core', 'fx_core_daily') }} where rate_date >= "2026-01-01"
