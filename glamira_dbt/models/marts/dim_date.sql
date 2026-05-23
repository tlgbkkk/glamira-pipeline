{{ config(materialized='table') }}

WITH date_bounds AS (
    SELECT
        MIN(actual_date) AS start_date,
        MAX(actual_date) AS end_date
    FROM {{ ref('stg_summary_raw') }}
    WHERE actual_date IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
    date_day                                             AS actual_date,
    FORMAT_DATE('%A', date_day)                         AS day_name,
    FORMAT_DATE('%B', date_day)                         AS month_name,
    EXTRACT(YEAR    FROM date_day)                      AS year_number,
    EXTRACT(QUARTER FROM date_day)                      AS quarter_number,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7)
         THEN TRUE ELSE FALSE END                       AS is_weekend
FROM date_bounds,
UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date_day