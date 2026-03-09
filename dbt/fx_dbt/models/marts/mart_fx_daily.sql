{{ config(materialized='table') }}

with eur_base as (
  select
    rate_date,
    'EUR' as base_currency,
    {% for ccy in var('fx_required_currencies') %}
    rate_{{ ccy|lower }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from {{ ref('stg_fx_rates_wide') }}
),

usd_base as (
  select
    rate_date,
    'USD' as base_currency,
    {% for ccy in var('fx_required_currencies') %}
    safe_divide(rate_{{ ccy|lower }}, rate_usd) as rate_{{ ccy|lower }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from {{ ref('stg_fx_rates_wide') }}
),

gbp_base as (
  select
    rate_date,
    'GBP' as base_currency,
    {% for ccy in var('fx_required_currencies') %}
    safe_divide(rate_{{ ccy|lower }}, rate_gbp) as rate_{{ ccy|lower }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from {{ ref('stg_fx_rates_wide') }}
)

select * from eur_base
union all
select * from usd_base
union all
select * from gbp_base
order by base_currency, rate_date desc
