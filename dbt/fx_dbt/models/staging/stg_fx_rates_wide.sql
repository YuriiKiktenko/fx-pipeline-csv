{{ config(materialized='view') }}

select
  rate_date,
  {% for ccy in var('fx_required_currencies') %}
  {% if ccy == var('fx_base_currency') %}
  cast(1.0 as numeric) as rate_{{ ccy|lower }}{% if not loop.last %},{% endif %}
  {% else %}
  max(if(currency = '{{ ccy }}', rate, null)) as rate_{{ ccy|lower }}{% if not loop.last %},{% endif %}
  {% endif %}
  {% endfor %}
from {{ ref('stg_fx_rates') }}
group by 1
order by 1 desc
