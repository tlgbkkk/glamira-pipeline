WITH source_ip_locations AS (
    SELECT
        _id
        ,ip
        ,region
        ,country
        ,city
    FROM {{ source('glamira_raw', 'ip_locations') }}
    WHERE _id IS NOT NULL
      AND ip IS NOT NULL
)

SELECT
    CAST(_id AS STRING) AS ip_location_id
    ,CAST(ip AS STRING) AS ip_address
    ,COALESCE(TRIM(CAST(country AS STRING)), 'unknown') AS country_name
    ,COALESCE(TRIM(CAST(region AS STRING)), 'unknown') AS region_name
    ,COALESCE(TRIM(CAST(city AS STRING)), 'unknown') AS city_name
FROM source_ip_locations