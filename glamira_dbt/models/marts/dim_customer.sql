{{
    config(
        materialized='incremental',
        unique_key='customer_key'
    )
}}

WITH source_data AS (
    SELECT
        customer_id,
        email_address AS email,
        order_timestamp AS change_date
    FROM {{ ref('stg_summary_raw') }}
    WHERE customer_id IS NOT NULL
      AND customer_id != ''
      AND email_address IS NOT NULL
),

ordered_data AS (
    SELECT
        *,
        LAG(email) OVER (
            PARTITION BY customer_id
            ORDER BY change_date
        ) AS prev_email
    FROM source_data
),

email_changes AS (
    SELECT
        customer_id,
        email,
        change_date AS start_date
    FROM ordered_data
    WHERE prev_email IS NULL
       OR prev_email != email
),

history AS (
    SELECT
        FARM_FINGERPRINT(
            CONCAT(
                customer_id,
                '|',
                email,
                '|',
                CAST(start_date AS STRING)
            )
        ) AS customer_key,

        customer_id,
        email,

        start_date,

        COALESCE(
            LEAD(start_date) OVER (PARTITION BY customer_id ORDER BY start_date),
            TIMESTAMP '9999-12-31 00:00:00'
        ) AS end_date
    FROM email_changes
),

final AS (
    SELECT
        customer_key,
        customer_id,
        email,
        start_date,
        end_date,
        end_date = TIMESTAMP '9999-12-31 00:00:00' AS is_current
    FROM history

    UNION ALL

    SELECT
        -1 AS customer_key,
        'UNKNOWN' AS customer_id,
        'unknown@unknown.com' AS email,
        TIMESTAMP '1900-01-01 00:00:00' AS start_date,
        TIMESTAMP '9999-12-31 00:00:00' AS end_date,
        TRUE AS is_current
)

SELECT
    f.*,

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
    ON f.customer_key = t.customer_key
{% endif %}