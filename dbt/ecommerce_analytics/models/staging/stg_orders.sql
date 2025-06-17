{{ config(materialized='view') }}

SELECT 
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date,
    status,
    EXTRACT(year FROM order_date) as order_year,
    EXTRACT(month FROM order_date) as order_month,
    EXTRACT(dow FROM order_date) as day_of_week,
    CASE 
        WHEN status = 'completed' THEN 1 
        ELSE 0 
    END as is_completed
FROM {{ source('ecommerce', 'orders') }}