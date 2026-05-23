WITH source_summary AS (
    SELECT
        _id
        ,time_stamp
        ,ip
        ,user_agent
        ,resolution
        ,user_id_db
        ,device_id
        ,store_id
        ,email_address
        ,product_id
        ,cat_id
        ,TO_JSON_STRING(option) AS product_options
    FROM {{ source('glamira_raw', 'summary_raw') }}
    WHERE _id IS NOT NULL
)

SELECT
    CAST(_id AS STRING) AS order_id
    ,TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)) AS order_timestamp
    ,CAST(ip AS STRING) AS ip_address
    ,CAST(user_agent AS STRING) AS user_agent
    ,COALESCE(TRIM(CAST(resolution AS STRING)), 'unknown') AS resolution
    ,CAST(user_id_db AS STRING) AS customer_id
    ,COALESCE(TRIM(CAST(device_id AS STRING)), 'unknown') AS device_code
    ,CAST(store_id AS INT64) AS store_id
    ,CAST(email_address AS STRING) AS email_address
    ,CAST(product_id AS STRING) AS product_id
    ,CAST(cat_id AS STRING) AS category_id
    ,product_options
    ,1 AS order_quantity
    ,0.0 AS discount_pct
FROM source_summary