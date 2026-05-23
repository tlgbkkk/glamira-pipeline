{{ config(materialized='table') }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_summary_raw') }}
),
dim_date AS (
    SELECT date_key, actual_date FROM {{ ref('dim_date') }}
),
dim_product AS (
    SELECT product_key, product_id, full_price, sale_price FROM {{ ref('dim_product') }}
),
dim_customer AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customer') }}
),
dim_device AS (
    SELECT device_key, device_code FROM {{ ref('dim_device') }}
),
dim_location AS (
    SELECT location_key, country_name, city_name, region_name
    FROM {{ ref('dim_location') }}
),
dim_exchange_currency AS (
    SELECT exchange_currency_key, currency, year, month
    FROM {{ ref('dim_exchange_currency') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.event_id', 's.product_id']) }} AS sales_order_detail_key,
    dd.date_key,
    dp.product_key,
    dc.customer_key,
    dl.location_key,
    dv.device_key,
    dec.exchange_currency_key,
    s.order_id,
    s.store_id,
    s.ip_address,
    s.price,
    s.order_quantity,
    CASE
        WHEN dp.full_price IS NOT NULL AND dp.full_price > 0 AND dp.sale_price IS NOT NULL
        THEN ROUND((dp.full_price - dp.sale_price) / dp.full_price, 4)
        ELSE 0.0
    END AS discount_pct,
    ROUND(
        s.price * s.order_quantity * (
            1 - CASE
                WHEN dp.full_price IS NOT NULL AND dp.full_price > 0 AND dp.sale_price IS NOT NULL
                THEN (dp.full_price - dp.sale_price) / dp.full_price
                ELSE 0.0
            END
        ), 2
    ) AS line_total

FROM stg s

LEFT JOIN dim_date dd
    ON s.actual_date = dd.actual_date

LEFT JOIN dim_product dp
    ON CAST(s.product_id AS STRING) = CAST(dp.product_id AS STRING)

LEFT JOIN dim_customer dc
    ON s.customer_id = dc.customer_id

LEFT JOIN dim_device dv
    ON s.device_code = dv.device_code

LEFT JOIN dim_location dl
    ON s.country_name = dl.country_name
    AND s.city_name   = dl.city_name
    AND s.region_name = dl.region_name

LEFT JOIN dim_exchange_currency dec
    ON s.currency_code                    = dec.currency
    AND EXTRACT(YEAR  FROM s.actual_date) = dec.year
    AND EXTRACT(MONTH FROM s.actual_date) = dec.month