/*
==============================================================================
Quality Checks
===============================================================================
Script Purpose: 
  This script performs the quality checks to validate the integrity, consistency,
  and accuracy of the Gold Layer. These checks ensure:
  -Uniqueness of surrogate keys in the dimension tables.
  -Referential integrity between fact and dimension tables.
  -Validation of relationships in the data model for analytical purposes.

Usage Notes:
  - Run these checks fter data loading Silver layer.
  - Investigate and resolve any discepancies found during the checks.
=====================================================================================================
*/

-- ==============================================================
-- Checking 'gold.dim_customers'
-- ==============================================================
-- Check for Uniqueness of customer key in gold.dim_customers
-- Expectation: no results
select 
customer_key,
count(*)as duplicate_count
from gold.dim_customers
group by customer_key
having count(*)>1;

-- ==============================================================
-- Checking 'gold.dim_products'
-- ==============================================================
-- Check for Uniqueness of product key in gold.dim_customers
-- Expectation: no results
select 
product_key,
count(*)as duplicate_count
from gold.dim_products
group by product_key
having count(*)>1;
-- ==============================================================
-- Checking 'gold.fact_sales'
-- ==============================================================
-- check the quality of the view

select * from gold.fact_sales
-- lets connect whole data model to check the issues
select * 
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key=f.customer_key
left join gold.dim_products p
on p.product_key=f.product_key
where c.customer_key is null
