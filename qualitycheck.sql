/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key

select cst_id,count(*) 
from bronze.crm_cust_info
Group by cst_id
HAVING count(*)> 1 OR cst_id IS NULL

-- Check for Unwanted Spaces
select cst_firstname 
FROM bronze.crm_cust_info
where cst_firstname <> TRIM(cst_firstname)

select cst_lastname 
FROM bronze.crm_cust_info
where cst_lastname <> TRIM(cst_lastname)


select cst_gndr
FROM bronze.crm_cust_info
where cst_gndr <> TRIM(cst_gndr)

select cst_marital_status
FROM bronze.crm_cust_info
where cst_marital_status <> TRIM(cst_marital_status)

-- Data Standardization & Consistency
SELECT DISTINCT cst_marital_status 
FROM bronze.crm_cust_info


SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key

select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 OR prd_id is null

-- Check for Unwanted Spaces
SELECT prd_nm 
FROM bronze.crm_prd_info
WHERE prd_nm <> trim(prd_nm )

-- Check for NULLs or Negative Values in Cost
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line 
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders (Start Date > End Date)
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt OR prd_end_dt IS NULL

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
select nullif(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0  or length(cast(sls_order_dt as varchar(50)))  < 8

	
select sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 or length(cast(sls_ship_dt as varchar(50)))  < 8

select sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or length(cast(sls_due_dt as varchar(50)))  < 8

select sls_order_dt, sls_ship_dt, sls_due_dt
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt OR  sls_order_dt  >sls_due_dt

-- ====================================================================
-- check negative and 0 for sales

select sls_sales, sls_quantity, sls_price
	from bronze.crm_sales_details
	where sls_sales <=0

-- Check Data Consistency: Sales = Quantity * Price
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
select bdate
from bronze.erp_cust_az12
where bdate > NOW()
-- Check for Unwanted Spaces
select cid 
from bronze.erp_cust_az12
where cid != trim(cid)

-- Data Standardization & Consistency
select distinct gen 
from bronze.erp_cust_az12

  -- check for foreign keys

select cid ,
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,length(cid))
	ELSE cid
	END as ncid	
from bronze.erp_cust_az12
where CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,length(cid))
	ELSE cid
	END 	 NOT IN (select  DISTINCT cst_key FROM silver.crm_cust_info)

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- QUALITY CHECK  replace
select cid 
from bronze. erp_loc_a101

----- check foriegn key
select 
REPLACE(cid, '-','') as cid,
cntry
from bronze. erp_loc_a101
where REPLACE(cid, '-','') NOT IN(select cst_key from silver.crm_cust_info)

---standardization
select distinct cntry from bronze. erp_loc_a101

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
