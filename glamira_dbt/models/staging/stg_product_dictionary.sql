WITH source_products AS (
    SELECT
        _id
        ,product_id
        ,sku
        ,name
        ,category
        ,collection
        ,type
        ,status
        ,url
        ,base_price
        ,full_price
        ,sale_price
        ,currency
        ,gold_weight
    FROM {{ source('glamira_raw', 'product_dictionary') }}
    WHERE _id IS NOT NULL
)

SELECT
    CAST(_id AS STRING) AS product_dict_obj_id
    ,CAST(product_id AS STRING) AS product_id
    ,CAST(sku AS STRING) AS sku
    ,CAST(name AS STRING) AS product_name
    ,CAST(category AS STRING) AS category
    ,CAST(collection AS STRING) AS collection
    ,CASE
        WHEN TRIM(type) IN ('--_select_--', '-1', '') THEN 'unknown'
        ELSE TRIM(type)
     END AS product_type
    ,CAST(status AS STRING) AS status
    ,CAST(url AS STRING) AS product_url
    ,CAST(base_price AS FLOAT64) AS base_price
    ,CAST(full_price AS FLOAT64) AS full_price
    ,CAST(sale_price AS FLOAT64) AS sale_price
    ,CAST(currency AS STRING) AS currency
    ,CAST(gold_weight AS FLOAT64) AS gold_weight
FROM source_products