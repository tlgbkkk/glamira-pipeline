WITH source_summary AS (
    SELECT *
    FROM {{ source('glamira_raw', 'summary_raw') }}
    WHERE _id IS NOT NULL
      AND collection = 'checkout_success'
),

cart_exploded AS (
    SELECT
        s._id,
        s.time_stamp,
        s.ip,
        s.user_agent,
        s.resolution,
        s.user_id_db,
        s.device_id,
        s.api_version,
        s.store_id,
        s.local_time,
        s.show_recommendation,
        s.current_url,
        s.referrer_url,
        s.email_address,
        s.collection,
        s.order_id,
        s.is_paypal,
        s.utm_source,
        s.utm_medium,
        s.option,
        s.key_search,
        s.viewing_product_id,
        s.recommendation,
        s.recommendation_clicked_position,
        s.recommendation_product_id,
        s.recommendation_product_position,
        cp.product_id   AS product_id,
        cp.price        AS price,
        cp.currency     AS currency,
        cp.amount       AS order_quantity,
        cp.option       AS cart_product_options
    FROM source_summary s,
    UNNEST(s.cart_products) AS cp
),

with_location AS (
    SELECT
        c.*,
        COALESCE(TRIM(CAST(il.country AS STRING)), 'unknown') AS country_name,
        COALESCE(TRIM(CAST(il.region  AS STRING)), 'unknown') AS region_name,
        COALESCE(TRIM(CAST(il.city    AS STRING)), 'unknown') AS city_name
    FROM cart_exploded c
    LEFT JOIN {{ source('glamira_raw', 'ip_locations') }} il
        ON c.ip = il.ip
)

SELECT
    CAST(_id AS STRING)                                         AS event_id,
    CAST(order_id AS STRING)                                    AS order_id,
    TO_HEX(SHA256(CAST(user_id_db AS STRING)))                  AS customer_id,
    CAST(product_id AS STRING)                                  AS product_id,
    CAST(store_id AS STRING)                                    AS store_id,
    TO_HEX(SHA256(CAST(device_id AS STRING)))                   AS device_code,
    TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))                AS order_timestamp,
    DATE(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)))          AS actual_date,
    CAST(local_time AS STRING)                                  AS local_time,
    CAST(price AS NUMERIC)                                      AS price,
    CAST(order_quantity AS INT64)                               AS order_quantity,
    TO_HEX(SHA256(CAST(ip AS STRING)))                          AS ip_address,
    CAST(user_agent AS STRING)                                  AS user_agent,
    COALESCE(TRIM(CAST(resolution AS STRING)), 'unknown')       AS resolution,
    CAST(api_version AS STRING)                                 AS api_version,
    TO_HEX(SHA256(CAST(email_address AS STRING)))               AS email_address,
    CAST(is_paypal AS BOOLEAN)                                  AS is_paypal,
    CAST(show_recommendation AS STRING)                         AS show_recommendation,
    CAST(current_url AS STRING)                                 AS current_url,
    CAST(referrer_url AS STRING)                                AS referrer_url,
    CAST(collection AS STRING)                                  AS collection_name,
    CAST(utm_source AS STRING)                                  AS utm_source,
    CAST(utm_medium AS STRING)                                  AS utm_medium,
    CAST(key_search AS STRING)                                  AS key_search,
    CAST(viewing_product_id AS STRING)                          AS viewing_product_id,
    CAST(recommendation AS STRING)                              AS recommendation,
    CAST(recommendation_clicked_position AS INT64)              AS recommendation_clicked_position,
    CAST(recommendation_product_id AS STRING)                   AS recommendation_product_id,
    CAST(recommendation_product_position AS INT64)              AS recommendation_product_position,
    TO_JSON_STRING(option)                                      AS product_options,
    TO_JSON_STRING(cart_product_options)                        AS cart_product_options,
    country_name,
    city_name,
    region_name,
    CASE
        WHEN currency = '€'     THEN 'EUR'
        WHEN currency = '£'     THEN 'GBP'
        WHEN currency = '¥'     THEN 'JPY'
        WHEN currency = '₩'     THEN 'KRW'
        WHEN currency = '₹'     THEN 'INR'
        WHEN currency = 'zł'    THEN 'PLN'
        WHEN currency = 'Kč'    THEN 'CZK'
        WHEN currency = 'Ft'    THEN 'HUF'
        WHEN currency = 'lei'   THEN 'RON'
        WHEN currency = 'din'   THEN 'RSD'
        WHEN currency = '₺'     THEN 'TRY'
        WHEN currency = 'R$'    THEN 'BRL'
        WHEN currency = 'Fr'    THEN 'CHF'
        WHEN currency = 'NZ$'   THEN 'NZD'
        WHEN currency = 'A$'    THEN 'AUD'
        WHEN currency = 'HK$'   THEN 'HKD'
        WHEN currency = 'S$'    THEN 'SGD'
        WHEN currency = 'CAD $' THEN 'CAD'
        WHEN currency = 'CN¥'   THEN 'CNY'
        WHEN currency = '₫'     THEN 'VND'
        WHEN currency = 'Rp'    THEN 'IDR'
        WHEN currency = '₱'     THEN 'PHP'
        WHEN currency = 'RM'    THEN 'MYR'
        WHEN currency = 'NT$'   THEN 'TWD'
        WHEN currency = 'kr' AND current_url LIKE '%glamira.dk%' THEN 'DKK'
        WHEN currency = 'kr' AND current_url LIKE '%glamira.no%' THEN 'NOK'
        WHEN currency = 'kr' AND current_url LIKE '%glamira.se%' THEN 'SEK'
        WHEN currency = 'kr' AND current_url LIKE '%glamira.is%' THEN 'ISK'
        WHEN currency = '$'  AND current_url LIKE '%glamira.com.au%' THEN 'AUD'
        WHEN currency = '$'  AND current_url LIKE '%glamira.ca%'     THEN 'CAD'
        WHEN currency = '$'  AND current_url LIKE '%glamira.nz%'     THEN 'NZD'
        WHEN currency = '$'  AND current_url LIKE '%glamira.sg%'     THEN 'SGD'
        WHEN currency = '$'  AND current_url LIKE '%glamira.hk%'     THEN 'HKD'
        WHEN currency = '$'                                          THEN 'USD'
        ELSE UPPER(TRIM(currency))
    END AS currency_code
FROM with_location