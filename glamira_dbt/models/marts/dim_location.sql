{{ config(materialized='table')}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['country_name', 'city_name', 'region_name']) }} AS location_key,
    country_name,
    city_name,
    region_name
FROM {{ ref('stg_ip_locations') }}