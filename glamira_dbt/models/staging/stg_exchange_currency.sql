WITH source_currency AS (
    SELECT
        month,
        year,
        currency,
        to_usd_rate
    FROM {{ ref('currency_exchange') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['year', 'month', 'currency']) }} AS exchange_rate_key,
    CAST(year AS INT64) AS year_number,
    CAST(month AS INT64) AS month_number,
    UPPER(TRIM(CAST(currency AS STRING))) AS currency_code,
    CAST(to_usd_rate AS FLOAT64) AS rate_to_usd
FROM source_currency