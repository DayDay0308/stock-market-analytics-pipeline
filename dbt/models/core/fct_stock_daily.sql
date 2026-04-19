{{ config(
    materialized='table',
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by=["exchange", "ticker"]
) }}

with staging as (
    select * from {{ ref('stg_stocks') }}
),

daily_metrics as (
    select
        date,
        ticker,
        exchange,
        open,
        high,
        low,
        close,
        volume,
        round(close - open, 4)                    as price_change,
        round((close - open) / nullif(open, 0) * 100, 4) as pct_change,
        round(high - low, 4)                       as daily_range,
        round(avg(close) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ), 4)                                      as moving_avg_30d,
        round(avg(close) over (
            partition by ticker
            order by date
            rows between 6 preceding and current row
        ), 4)                                      as moving_avg_7d,
        round(stddev(close) over (
            partition by ticker
            order by date
            rows between 29 preceding and current row
        ), 4)                                      as volatility_30d
    from staging
)

select * from daily_metrics