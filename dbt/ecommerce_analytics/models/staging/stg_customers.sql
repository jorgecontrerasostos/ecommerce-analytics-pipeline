{{ config(materialized='view') }}

SELECT 
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    phone,
    created_at,
    CURRENT_DATE - created_at::date as days_since_signup
FROM {{ source('ecommerce', 'customers') }}