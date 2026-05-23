{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    product_id,
    sku AS sku_code,
    product_name,
    category AS category_name,
    collection AS collection_name,
    product_type,
    base_price,
    full_price,
    sale_price,
    currency,
    gold_weight
FROM {{ ref('stg_product_dictionary')}}