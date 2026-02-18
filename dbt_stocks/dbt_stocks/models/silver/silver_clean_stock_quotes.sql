SELECT
    symbol,
    current_price,
    change_amount,
    change_percent,
    day_high,
    day_low,
    day_open,
    prev_close,
    market_timestamp,
    fetched_at
FROM {{ ref('bronze_stg_stock_quotes') }}
WHERE current_price IS NOT NULL
  AND symbol IS NOT NULL
