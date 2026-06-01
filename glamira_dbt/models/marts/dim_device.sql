{{
    config(
        materialized='incremental',
        unique_key='device_key'
    )
}}

WITH source AS (
    SELECT
        FARM_FINGERPRINT(device_code) AS device_key,
        device_code,
        user_agent,
        resolution
    FROM {{ ref('stg_summary_raw') }}
    WHERE device_code IS NOT NULL
      AND device_code != 'unknown'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY device_code
        ORDER BY order_timestamp DESC
    ) = 1
),

final AS (
    SELECT * FROM source

    UNION ALL

    SELECT
        -1 AS device_key,
        'UNKNOWN' AS device_code,
        'Unknown' AS user_agent,
        'Unknown' AS resolution
)

SELECT
    f.device_key,
    f.device_code,
    f.user_agent,
    f.resolution,
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
    ON f.device_key = t.device_key
{% endif %}