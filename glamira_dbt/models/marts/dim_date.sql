{{
    config(
        materialized='incremental',
        unique_key='date_key'
    )
}}

WITH date_bounds AS (
    SELECT
        MIN(actual_date) AS start_date,
        MAX(actual_date) AS end_date
    FROM {{ ref('stg_summary_raw') }}
    WHERE actual_date IS NOT NULL
),

source_data AS (
    SELECT
        FARM_FINGERPRINT(CAST(date_day AS STRING)) AS date_key,
        date_day AS actual_date,
        FORMAT_DATE('%A', date_day) AS day_name,
        FORMAT_DATE('%B', date_day) AS month_name,
        EXTRACT(YEAR FROM date_day) AS year_number,
        EXTRACT(QUARTER FROM date_day) AS quarter_number,
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1,7)
            THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM date_bounds,
    UNNEST(
        GENERATE_DATE_ARRAY(
            start_date,
            end_date,
            INTERVAL 1 DAY
        )
    ) AS date_day
),

final AS (
    SELECT
        -1 AS date_key,
        DATE('1970-01-01') AS actual_date,
        'Unknown' AS day_name,
        'Unknown' AS month_name,
        0 AS year_number,
        0 AS quarter_number,
        FALSE AS is_weekend

    UNION ALL

    SELECT *
    FROM source_data
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
    ON f.date_key = t.date_key
{% endif %}