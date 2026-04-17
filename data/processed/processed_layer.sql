-- =====================================================
-- FILE: 01_processed_layer.sql
-- PURPOSE: Clean and transform raw e-commerce data
-- LAYER: PROCESSED (Staging + Transformation)
-- =====================================================

-- This query:
-- 1. Standardizes data types
-- 2. Handles NULL values safely
-- 3. Joins orders with order items
-- 4. Aggregates order-level metrics

CREATE OR REPLACE TABLE `de-project-493506.analytics_db.processed_ecommerce_sales_customer_analytics` AS

WITH orders AS (
    -- Clean orders table
    SELECT
        SAFE_CAST(order_id AS INT64) AS order_id,
        SAFE_CAST(user_id AS INT64) AS user_id,
        SAFE_CAST(order_date AS DATE) AS order_date
    FROM `de-project-493506.analytics_db.order_transactions`
),

order_items AS (
    -- Clean order items table
    SELECT
        SAFE_CAST(order_id AS INT64) AS order_id,
        SAFE_CAST(product_id AS INT64) AS product_id,
        IFNULL(SAFE_CAST(quantity AS INT64), 0) AS quantity,
        IFNULL(SAFE_CAST(price AS FLOAT64), 0.0) AS price
    FROM `de-project-493506.analytics_db.order_line_items`
),

aggregated AS (
    -- Aggregate order metrics
    SELECT
        o.user_id,
        o.order_id,
        o.order_date,
        SUM(oi.quantity) AS total_quantity,
        SUM(oi.quantity * oi.price) AS total_revenue
    FROM orders o
    LEFT JOIN order_items oi
        ON o.order_id = oi.order_id
    GROUP BY
        o.user_id,
        o.order_id,
        o.order_date
)

-- Final cleaned output
SELECT
    SAFE_CAST(user_id AS INT64) AS user_id,
    SAFE_CAST(order_id AS INT64) AS order_id,
    SAFE_CAST(order_date AS DATE) AS order_date,
    IFNULL(SAFE_CAST(total_quantity AS INT64), 0) AS total_quantity,
    IFNULL(SAFE_CAST(total_revenue AS FLOAT64), 0.0) AS total_revenue
FROM aggregated;