{{ config(materialized='table')}}

SELECT
    p.category,
    COUNT(o.order_id) as total_orders,
    SUM(o.complete_order_value) as total_revenue,
    AVG(o.order_value) as avg_order_value,
    SUM(o.quantity) as total_quantity_sold,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM {{ ref('fct_orders') }} o
JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id
WHERE o.is_completed = 1
GROUP BY p.category
ORDER BY total_revenue DESC