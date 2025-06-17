{{ config(materialized='table')}}

SELECT
    order_year,
    order_month,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN is_completed = 1 THEN 1 END) as completed_orders,
    SUM(order_value) as total_revenue,
    SUM(complete_order_value) as completed_revenue,
    AVG(order_value) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM {{ ref('fct_orders') }}
GROUP BY order_year, order_month
ORDER BY order_year, order_month