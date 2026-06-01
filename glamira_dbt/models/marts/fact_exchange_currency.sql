{{
    config(
        materialized='incremental',
        unique_key='exchange_currency_key'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_exchange_currency') }}
),

dim_currency AS (
    SELECT
        currency_key,
        currency_code
    FROM {{ ref('dim_currency') }}
),

final AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(s.currency_code, CAST(s.year_number AS STRING), CAST(s.month_number AS STRING))) AS exchange_currency_key,
        s.month_number,
        s.year_number,
        dc.currency_key,
        s.rate_to_usd
    FROM source s
    LEFT JOIN dim_currency dc
        ON s.currency_code = dc.currency_code
)

SELECT
    f.exchange_currency_key,
    COALESCE(f.currency_key, -1) AS currency_key,
    f.month_number,
    f.year_number,
    f.rate_to_usd,
    {% if is_incremental() %}
        COALESCE(t.created_at, CURRENT_TIMESTAMP()) AS created_at,
        COALESCE(t.created_by, SESSION_USER()) AS created_by,
    {% else %}
        CURRENT_TIMESTAMP() AS created_at,
        SESSION_USER() AS created_by,
    {% endif %}
    CURRENT_TIMESTAMP() AS updated_at,
    SESSION_USER() AS updated_by
FROM final f
{% if is_incremental() %}
LEFT JOIN {{this}} t
    ON f.exchange_currency_key = t.exchange_currency_key
{% endif %}