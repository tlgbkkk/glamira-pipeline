SELECT
    exchange_rate_key,
    year,
    month,
    currency,
    to_usd_rate
FROM {{ ref('stg_currency_exchange')}}