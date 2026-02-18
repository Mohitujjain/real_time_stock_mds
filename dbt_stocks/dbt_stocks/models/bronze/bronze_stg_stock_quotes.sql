SELECT
    "DATA":symbol::string AS symbol,
    "DATA":c::float AS current_price,
    "DATA":d::float AS change_amount,
    "DATA":dp::float AS change_percent,
    "DATA":h::float AS day_high,
    "DATA":l::float AS day_low,
    "DATA":o::float AS day_open,
    "DATA":pc::float AS prev_close,
    TO_TIMESTAMP_NTZ("DATA":t) AS market_timestamp,
    CURRENT_TIMESTAMP() AS fetched_at
FROM {{ source('raw', 'BRONZE_STOCK_QUOTES_RAW') }}
