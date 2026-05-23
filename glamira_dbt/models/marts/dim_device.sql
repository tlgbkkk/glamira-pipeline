{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['device_code']) }} AS device_key,
    device_code,
    user_agent,
    resolution
FROM {{ ref('stg_summary_raw') }}
WHERE device_code IS NOT NULL
  AND device_code != 'unknown'
QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY order_timestamp DESC) = 1