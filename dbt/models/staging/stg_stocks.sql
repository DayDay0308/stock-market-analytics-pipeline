{{ config(materialized='view') }}

select
    cast(date as date)          as date,
    ticker,
    exchange,
    cast(open as float64)       as open,
    cast(high as float64)       as high,
    cast(low as float64)        as low,
    cast(close as float64)      as close,
    cast(volume as float64)     as volume
from {{ source('stock_market_data', 'raw_stocks_partitioned') }}
where close is not null
  and ticker is not null
  and date is not null