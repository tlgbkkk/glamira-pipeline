{{
    config(
        materialized='incremental',
        unique_key='location_key'
    )
}}

WITH source_data AS (
    SELECT
        FARM_FINGERPRINT(
            CONCAT(
                COALESCE(country_name,''),
                '|',
                COALESCE(city_name,''),
                '|',
                COALESCE(region_name,'')
            )
        ) AS location_key,
        country_name,
        city_name,
        region_name
    FROM {{ ref('stg_ip_locations') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY country_name, city_name, region_name
        ORDER BY country_name
    ) = 1
),

final AS (
    SELECT
        -1 AS location_key,
        'Unknown Country' AS country_name,
        'Unknown City' AS city_name,
        'Unknown Region' AS region_name

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
    ON f.location_key = t.location_key
{% endif %}