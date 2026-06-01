{{
    config(
        materialized='incremental',
        unique_key='product_key'
    )
}}

WITH source_data AS (

    SELECT
        FARM_FINGERPRINT(COALESCE(CAST(product_id AS STRING), 'UNKNOWN')) AS product_key,
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
    FROM {{ ref('stg_product_dictionary') }}

),

final AS (
    -- null case
    SELECT
        -1 AS product_key,
        'UNKNOWN' AS product_id,
        'UNKNOWN' AS sku_code,
        'Unknown Product' AS product_name,
        'Unknown Category' AS category_name,
        'Unknown Collection' AS collection_name,
        'Unknown Type' AS product_type,
        0 AS base_price,
        0 AS full_price,
        0 AS sale_price,
        'UNKNOWN' AS currency,
        0 AS gold_weight

    UNION ALL

    SELECT *
    FROM source_data
)

SELECT
    f.product_key,
    f.product_id,
    f.sku_code,
    f.product_name,
    f.category_name,
    f.collection_name,
    f.product_type,
    f.base_price,
    f.full_price,
    f.sale_price,
    f.currency,
    f.gold_weight,

    {% if is_incremental() %}
        COALESCE(t.created_at, CURRENT_TIMESTAMP()) AS created_at,
        COALESCE(t.created_by, SESSION_USER())      AS created_by,
    {% else %}
        CURRENT_TIMESTAMP() AS created_at,
        SESSION_USER()      AS created_by,
    {% endif %}

    CURRENT_TIMESTAMP() AS updated_at,
    SESSION_USER()      AS updated_by

FROM final f

{% if is_incremental() %}
LEFT JOIN {{ this }} t
    ON f.product_key = t.product_key
{% endif %}