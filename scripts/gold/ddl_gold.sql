/*
====================================================================================
DDL Script: Create Gold Views
=====================================================================================
Scrip Purpose:
The script creates views for the Gold layer in the data warehouse.
The Gold Layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and ciombines data from the Silver layer to
produce a claen , enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reprting.
===================================================================================
*/
--===============================================================================
-- Create Dimension: gold.dim_customers
select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_material_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid

-- Now as we are joining different tables
-- our concern is to make sure that we dont have duplicates
select cst_id,count(*) from 
(select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_material_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
)t group by cst_id
having count(*)>1
-- This male sure that we do not have any duplicates.

-- Now we have integration issues
select 
distinct
ci.cst_gndr,
ca.gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
order by 1,2
-- There is some inconsistency
select 
distinct
ci.cst_gndr,
ca.gen,
-- as CRM is the master for gender info
case when ci.cst_gndr!='n/a' then ci.cst_gndr
else coalesce(ca.gen,'n/a')
end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
order by 1,2
-- so the final one
select 
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_material_status as marital_status,
case when ci.cst_gndr!='n/a' then ci.cst_gndr
else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
-- as most of the columns are having details of the customer, not the factual infrmation 
-- like transaction, sales and so on so, it is dimension table
-- for Dimension you should have primary key
-- It can be customer_id
-- Primary key can also be surrogate key
-- let's generate the surrogate key
select 
row_number() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_material_status as marital_status,
case when ci.cst_gndr!='n/a' then ci.cst_gndr
else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
-- Now we have to create the object
-- As object would be virtual
create view gold.dim_customers as 
select 
row_number() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_material_status as marital_status,
case when ci.cst_gndr!='n/a' then ci.cst_gndr
else coalesce(ca.gen,'n/a')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid

-- Now will check the standardization of the view
select * from gold.dim_customers;
select distinct gender from gold.dim_customers;
=======================================================================
-- Create Dimension: gold.dim_products
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
from silver.crm_prd_info as pn
-- as prd_key is the primary key and we want to keep current data
-- filter out all historical data
where prd_end_dt is null;

-- now we can remove prd_end_dt as it is always null
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
from silver.crm_prd_info as pn
where prd_end_dt is null;

-- and this is related to erp_px_cat_g1v2 table, so check the table
select * from silver.erp_px_cat_g1v2

-- join iot with erp_px_cat_g1v2
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null;

-- Check the quality & uniqueness
select prd_key,count(*) from 
(select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.cat,
pc.subcat,
pc.maintenance
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null)t
group by prd_key
having count(*)>1
-- so, all are unique
-- so, final one is
select 
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null;

-- its the detail of the product. so, it is also dimension table
-- generate the primary key
select 
row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null;
-- create view
create view gold.dim_products as 
select 
row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id=pc.id
where pn.prd_end_dt is null;

-- Now will check the standardization of the view
select * from gold.dim_products
===================================================================================
-- Create Fact : gold.fact_sales
select 
sd.sls_ord_num,
sd.sls_prd_key,
sd.sls_cust_id,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
from silver.crm_sales_details sd
-- we have transactions, price, and lot of factual information so it would be our fact table.

-- join fact table with dimension table
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key= pr.product_number
left join gold.dim_customers cu 
on sd.sls_cust_id=cu.customer_id

-- create view
create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key= pr.product_number
left join gold.dim_customers cu 
on sd.sls_cust_id=cu.customer_id

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
=================================================================================================
