{{ config(materialized='view') }}

SELECT 
    product_id,
    product_name,
    category,
    price,
    created_at,
    CASE 
        WHEN price < 50 THEN 'Low'
        WHEN price < 200 THEN 'Medium' 
        ELSE 'High'
    END as price_tier
FROM {{ source('ecommerce', 'products') }}