WITH source_summary AS (
    SELECT *
    FROM {{ source('glamira_raw', 'summary_raw') }}
    WHERE _id IS NOT NULL
      AND collection = 'checkout_success'
),

deduped_source AS (
    SELECT *
    FROM source_summary
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(order_id AS STRING)
        ORDER BY time_stamp DESC
    ) = 1
),

aggregated_cart AS (
    SELECT
        s._id,
        cp.product_id,
        cp.price,
        ANY_VALUE(COALESCE(cp.currency, s.currency)) AS currency,
        SUM(COALESCE(SAFE_CAST(cp.amount AS INT64), 1)) AS order_quantity,
        ANY_VALUE(cp.option) AS cart_product_options
    FROM deduped_source s
    CROSS JOIN UNNEST(s.cart_products) cp
    GROUP BY s._id, cp.product_id, cp.price
),

cart_exploded AS (
    SELECT
        s.* EXCEPT(cart_products, product_id, price, currency),
        c.product_id,
        c.price,
        c.currency,
        c.order_quantity,
        c.cart_product_options
    FROM deduped_source s
    JOIN aggregated_cart c
        ON s._id = c._id
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
    CAST(CAST(order_id AS FLOAT64) AS INT64)                    AS order_id,
    CAST(user_id_db AS STRING)                                  AS customer_id,
    CAST(product_id AS STRING)                                  AS product_id,
    CAST(CAST(store_id AS FLOAT64) AS INT64)                    AS store_id,
    CAST(device_id AS STRING)                                   AS device_code,
    TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))                AS order_timestamp,
    DATE(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)))          AS actual_date,
    CAST(local_time AS STRING)                                  AS local_time,
    CAST(price AS NUMERIC)                                      AS price,
    CAST(order_quantity AS INT64)                               AS order_quantity,
    CAST(ip AS STRING)                                          AS ip_address,
    CAST(user_agent AS STRING)                                  AS user_agent,
    COALESCE(TRIM(CAST(resolution AS STRING)), 'unknown')       AS resolution,
    CAST(api_version AS STRING)                                 AS api_version,
    CAST(email_address AS STRING)                               AS email_address,
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
        WHEN currency = '€'         THEN 'EUR'
        WHEN currency = '£'         THEN 'GBP'
        WHEN currency = '¥'         THEN 'JPY'
        WHEN currency = '円'         THEN 'JPY'
        WHEN currency = '₩'         THEN 'KRW'
        WHEN currency = '₹'         THEN 'INR'
        WHEN currency = 'zł'        THEN 'PLN'
        WHEN currency = 'Kč'        THEN 'CZK'
        WHEN currency = 'Ft'        THEN 'HUF'
        WHEN currency = 'lei'       THEN 'RON'
        WHEN currency = 'Lei'       THEN 'RON'
        WHEN currency = 'din'       THEN 'RSD'
        WHEN currency = ' din.'     THEN 'RSD'
        WHEN currency = 'лв.'       THEN 'BGN'
        WHEN currency = '₺'         THEN 'TRY'
        WHEN currency = 'R$'        THEN 'BRL'
        WHEN currency = 'Fr'        THEN 'CHF'
        WHEN currency = 'CHF'       THEN 'CHF'
        WHEN currency = 'NZ$'       THEN 'NZD'
        WHEN currency = 'NZD $'     THEN 'NZD'
        WHEN currency = 'A$'        THEN 'AUD'
        WHEN currency = 'AU $'      THEN 'AUD'
        WHEN currency = 'HK$'       THEN 'HKD'
        WHEN currency = 'HKD $'     THEN 'HKD'
        WHEN currency = 'S$'        THEN 'SGD'
        WHEN currency = 'SGD $'     THEN 'SGD'
        WHEN currency = 'CAD $'     THEN 'CAD'
        WHEN currency = 'CN¥'       THEN 'CNY'
        WHEN currency = '₫'         THEN 'VND'
        WHEN currency = 'Rp'        THEN 'IDR'
        WHEN currency = '₱'         THEN 'PHP'
        WHEN currency = 'RM'        THEN 'MYR'
        WHEN currency = 'NT$'       THEN 'TWD'
        WHEN currency = 'MXN $'     THEN 'MXN'
        WHEN currency = 'CLP'       THEN 'CLP'
        WHEN currency = 'COP $'     THEN 'COP'
        WHEN currency = 'PEN S/.'   THEN 'PEN'
        WHEN currency = 'GTQ Q'     THEN 'GTQ'
        WHEN currency = 'CRC ₡'     THEN 'CRC'
        WHEN currency = 'USD $'     THEN 'USD'
        WHEN currency = 'UYU'       THEN 'UYU'
        WHEN currency = '₲'         THEN 'PYG'
        WHEN currency = 'BOB Bs'    THEN 'BOB'
        WHEN currency = 'DOP $'     THEN 'DOP'
        WHEN currency = 'kn'        THEN 'HRK'
        WHEN currency = 'د.ك.‏'      THEN 'KWD'
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