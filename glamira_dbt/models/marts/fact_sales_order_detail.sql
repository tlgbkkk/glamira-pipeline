{{
    config(
        materialized='incremental',
        unique_key='sales_order_detail_key'
    )
}}

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

ip_locations AS (
    SELECT ip_address, country_name, city_name, region_name
    FROM {{ ref('stg_ip_locations') }}
),

dim_currency AS (
    SELECT currency_key, currency_code FROM {{ ref('dim_currency') }}
),

final AS (
    SELECT
        FARM_FINGERPRINT(
          CONCAT(
            CAST(s.order_id AS STRING),
            '|',
            COALESCE(s.product_id, '-1'),
            '|',
            CAST(COALESCE(s.price, 0) AS STRING)
          )
        ) AS sales_order_detail_key,
        COALESCE(dd.date_key, -1) AS date_key,
        COALESCE(dp.product_key, -1) AS product_key,
        COALESCE(dc.customer_key, -1) AS customer_key,
        COALESCE(dl.location_key, -1) AS location_key,
        COALESCE(dv.device_key, -1) AS device_key,
        COALESCE(dcur.currency_key, -1) AS currency_key,
        s.order_id,
        s.store_id,
        s.ip_address,
        COALESCE(s.price, 0) AS price,
        s.order_quantity,
        ROUND(s.price * s.order_quantity, 2) AS line_total
    FROM stg s
    LEFT JOIN dim_date dd
        ON s.actual_date = dd.actual_date
    LEFT JOIN dim_product dp
        ON CAST(s.product_id AS STRING) = CAST(dp.product_id AS STRING)
    LEFT JOIN dim_customer dc
        ON s.customer_id = dc.customer_id
    LEFT JOIN dim_device dv
        ON s.device_code = dv.device_code
    LEFT JOIN ip_locations il
        ON s.ip_address = TO_HEX(SHA256(CAST(il.ip_address AS STRING)))
    LEFT JOIN dim_location dl
        ON il.country_name = dl.country_name
        AND il.city_name   = dl.city_name
        AND il.region_name = dl.region_name
    LEFT JOIN dim_currency dcur
        ON s.currency_code = dcur.currency_code
)

SELECT
    f.sales_order_detail_key,
    f.date_key,
    f.product_key,
    f.customer_key,
    f.location_key,
    f.device_key,
    f.currency_key,
    f.order_id,
    f.store_id,
    f.ip_address,
    f.price,
    f.order_quantity,
    f.line_total,
    {% if is_incremental() %}
        COALESCE(t.created_at, CURRENT_TIMESTAMP()) AS created_at,
        COALESCE(t.created_by, SESSION_USER())       AS created_by,
    {% else %}
        CURRENT_TIMESTAMP() AS created_at,
        SESSION_USER()       AS created_by,
    {% endif %}
    CURRENT_TIMESTAMP() AS updated_at,
    SESSION_USER()       AS updated_by
FROM final f
{% if is_incremental() %}
LEFT JOIN {{ this }} t
    ON f.sales_order_detail_key = t.sales_order_detail_key
{% endif %}