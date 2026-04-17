-- =====================================================
-- FILE: 02_final_layer.sql
-- PURPOSE: Create business-ready analytics table
-- LAYER: FINAL
-- =====================================================

-- This query:
-- 1. Selects only clean, validated fields
-- 2. Prepares data for reporting and dashboards

CREATE OR REPLACE TABLE `de-project-493506.analytics_db.final_ecommerce_sales_customer_analytics` AS

SELECT
    user_id,
    order_id,
    order_date,
    total_quantity,
    total_revenue
FROM `de-project-493506.analytics_db.processed_ecommerce_sales_customer_analytics`;