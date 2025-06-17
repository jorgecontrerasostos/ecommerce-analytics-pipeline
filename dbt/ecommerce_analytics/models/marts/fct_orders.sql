{{ config(materialized='table') }}

SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.order_date,
    o.status,
    o.order_year,
    o.order_month,
    o.is_completed,
    p.price as unit_price,
    (o.quantity * p.price) as order_value,
    CASE
        WHEN o.is_completed = 1 THEN (o.quantity * p.price)
    ELSE 0
    END as complete_order_value
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id