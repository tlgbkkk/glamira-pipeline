{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    email_address AS email
FROM {{ ref('stg_summary_raw') }}
WHERE customer_id IS NOT NULL
  AND customer_id != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_timestamp DESC) = 1