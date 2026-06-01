{{
    config(
        materialized='incremental',
        unique_key='currency_key'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        FARM_FINGERPRINT(currency_code) AS currency_key,
        currency_code
    FROM {{ ref('stg_exchange_currency') }}
),

final AS (
    SELECT
        -1 AS currency_key,
        'UNKNOWN' AS currency_code

    UNION ALL

    SELECT *
    FROM source_data
)

SELECT
    f.currency_key,
    f.currency_code,

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
LEFT JOIN {{ this }} t
    ON f.currency_key = t.currency_key
{% endif %}