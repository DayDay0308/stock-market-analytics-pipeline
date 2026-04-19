{{ config(materialized='table') }}

with daily as (
    select * from {{ ref('fct_stock_daily') }}
),

summary as (
    select
        exchange,
        date,
        count(distinct ticker)          as num_stocks,
        round(avg(close), 4)            as avg_close_price,
        round(avg(pct_change), 4)       as avg_pct_change,
        round(sum(volume), 0)           as total_volume,
        round(avg(volatility_30d), 4)   as avg_volatility,
        round(max(close), 4)            as max_close,
        round(min(close), 4)            as min_close
    from daily
    group by exchange, date
)

select * from summary