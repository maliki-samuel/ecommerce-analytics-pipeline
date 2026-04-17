# E-Commerce Data Pipeline for Sales and Customer Analytics  

## Overview  

This project models a real-world data engineering workflow where raw e-commerce data from multiple sources is transformed into a structured dataset for analysis.

The objective is to produce a reliable table that answers a core business question:

How much revenue does each order generate?

The pipeline follows an ELT approach. Raw data is first loaded into BigQuery, and all transformations are performed using SQL inside the data warehouse.

---

## What the Pipeline Does  

- Ingests 9 raw data files (CSV and JSON)  
- Loads data into BigQuery raw tables  
- Joins order-level and item-level datasets  
- Computes:
  - total quantity per order  
  - total revenue per order  
- Produces a final analytics table ready for querying  

---

## Tech Stack  

| Area           | Tool                        |
|----------------|-----------------------------|
| Language       | Python                      |
| Data Warehouse | Google BigQuery             |
| Transformation | SQL (BigQuery Standard SQL) |
| Data Formats   | CSV, JSON                   |

---

## Pipeline Design  

Raw Files → Ingestion Script → BigQuery Raw Tables → Processed Layer → Final Table  

The pipeline is organized into clear layers:

- Ingestion: Python loads raw files into BigQuery  
- Raw layer: Data is stored without modification  
- Processed layer: Joins and aggregations are applied  
- Final layer: Data is standardized for querying  

---

## Key Design Decisions  

### ELT over ETL  

Transformations are performed in BigQuery instead of Python. This keeps the pipeline simple and leverages the processing power of the data warehouse.

### Layered Transformations  

The transformation logic is split into two stages:

- Processed layer: handles joins and aggregations  
- Final layer: enforces consistent data types  

This separation improves readability and maintainability.

### Defensive SQL  

To handle inconsistent data:

- SAFE_CAST prevents type conversion errors  
- IFNULL ensures numeric fields do not return null values  

---

## Transformation Logic  

### Processed Layer  

- Join order_transactions with order_line_items on order_id  
- Compute:
  - total_quantity = SUM(quantity)  
  - total_revenue = SUM(quantity * price)  

### Final Layer  

- Apply consistent data types  
- Output a clean, query-ready table  

---

## Run the Pipeline  

Load data into BigQuery:

python scripts/load_to_bigquery.py

Run transformations:

bq query --use_legacy_sql=false < sql/processed_layer.sql  
bq query --use_legacy_sql=false < sql/final_layer.sql  

Query final table:

SELECT *  
FROM `de-project-493506.analytics_db.final_ecommerce_sales_customer_analytics`  
LIMIT 10;  

---

## What This Project Demonstrates  

- Understanding of ELT pipeline design  
- Ability to structure data into clear layers  
- Writing SQL for joins and aggregations  
- Handling imperfect data safely  
- Building a pipeline that is simple and extensible  

---

## Author  

Maliki Habib Samuel
