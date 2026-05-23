SELECT
    {{ dbt_utils.generate_surrogate_key(['year', 'month', 'currency']) }} AS exchange_currency_key,
    year,
    month,
    currency,
    to_usd_rate
FROM {{ ref('stg_exchange_currency')}}