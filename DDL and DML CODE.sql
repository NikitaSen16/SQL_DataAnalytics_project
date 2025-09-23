/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	

*/

DROP DATABASE IF EXISTS "DataWarehouse";
CREATE DATABASE "DataWarehouse";
-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;


/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key	VARCHAR(50),
cst_firstname	VARCHAR(50),
cst_lastname	VARCHAR(50),
cst_marital_status	VARCHAR(50),
cst_gndr	VARCHAR(50),
cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,	
prd_line VARCHAR(50),
prd_start_dt TIMESTAMP,
prd_end_dt TIMESTAMP

	
);
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,	
sls_ship_dt INT,
sls_due_dt	INT,
sls_sales INT,
sls_quantity INT,
sls_price INT

	
);
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
CID	VARCHAR(50),
BDATE DATE,	
GEN VARCHAR(50)

);
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
CID VARCHAR(50),
CNTRY VARCHAR(50)

);
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
ID VARCHAR(50),
CAT VARCHAR(50),
SUBCAT	VARCHAR(50),
MAINTENANCE VARCHAR(50)

	
);

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.
===============================================================================
*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS
$$
DEClARE
	v_row_count INT;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN

		batch_start_time := NOW();
		RAISE NOTICE '----------------------------';
		RAISE NOTICE 'Loading Bronze Layer';
		RAISE NOTICE '----------------------------';
	
	
		RAISE NOTICE'+++++++++++++++++++++++++++++';
		RAISE NOTICE'Loading CRM tables';
		RAISE NOTICE'+++++++++++++++++++++++++++++';

		start_time := NOW();
		RAISE NOTICE'>>>Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		RAISE NOTICE'>>>inserting data into table: bronze.crm_cust_info';
		COPY bronze.crm_cust_info
		FROM 'C:\pg_data\datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (FORMAT csv, HEADER true);
		end_time := NOW();
		RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		SELECT COUNT(*) INTO v_row_count FROM bronze.crm_cust_info;
    	RAISE NOTICE 'Inserted % rows into bronze.crm_cust_info', v_row_count;



		start_time := NOW();
		RAISE NOTICE'>>>Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		RAISE NOTICE'>>>inserting data into table: bronze.crm_prd_info';
		COPY bronze.crm_prd_info
		FROM 'C:\pg_data\datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (FORMAT csv, HEADER true);

		end_time := NOW();
		RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		SELECT COUNT(*) INTO v_row_count FROM bronze.crm_prd_info;
    	RAISE NOTICE 'Inserted % rows into bronze.crm_prd_info', v_row_count;
	



		start_time := NOW();
		RAISE NOTICE'>>>Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
	
		RAISE NOTICE'>>>inserting data into table: bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM 'C:\pg_data\datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (FORMAT csv, HEADER true);

		end_time := NOW();
		RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		SELECT COUNT(*) INTO v_row_count FROM bronze.crm_sales_details;
    	RAISE NOTICE 'Inserted % rows into bronze.crm_sales_details', v_row_count;

	
		RAISE NOTICE'+++++++++++++++++++++++++++++';
		RAISE NOTICE'Loading ERP tables';
		RAISE NOTICE'+++++++++++++++++++++++++++++';
	

		start_time := NOW();
		RAISE NOTICE'>>>Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		
		RAISE NOTICE'>>>inserting data into table: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM 'C:\pg_data\datawarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (FORMAT csv, HEADER true);

		end_time := NOW();
		RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));	
		SELECT COUNT(*) INTO v_row_count FROM bronze.erp_cust_az12;
    	RAISE NOTICE 'Inserted % rows into bronze.erp_cust_az12', v_row_count;




		start_time := NOW();
		RAISE NOTICE'>>>Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		RAISE NOTICE'>>>inserting data into table: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM 'C:\pg_data\datawarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (FORMAT csv, HEADER true);

		end_time := NOW();
		RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		SELECT COUNT(*) INTO v_row_count FROM bronze.erp_loc_a101;
    	RAISE NOTICE 'Inserted % rows into bronze.erp_loc_a101', v_row_count;




		start_time := NOW();
		RAISE NOTICE'>>>Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		RAISE NOTICE'>>>inserting data into table: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\pg_data\datawarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (FORMAT csv, HEADER true);

		end_time := NOW();
		RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
		SELECT COUNT(*) INTO v_row_count FROM bronze.erp_px_cat_g1v2;
    	RAISE NOTICE 'Inserted % rows into bronze.erp_px_cat_g1v2', v_row_count;

		batch_end_time := NOW();
		RAISE NOTICE '>>> total Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
	
	EXCEPTION
    WHEN OTHERS THEN
    RAISE NOTICE 'Error Message: %', SQLERRM;
	
END;
$$
/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
===============================================================================
*/

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key	VARCHAR(50),
cst_firstname	VARCHAR(50),
cst_lastname	VARCHAR(50),
cst_marital_status	VARCHAR(50),
cst_gndr	VARCHAR(50),
cst_create_date DATE,
dwh_create_date TIMESTAMP DEFAULT now()
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id INT,
cat_id VARCHAR(50),
prd_key VARCHAR(50),
prd_nm VARCHAR(50),
prd_cost INT,	
prd_line VARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date TIMESTAMP DEFAULT now()

	
);
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,	
sls_ship_dt DATE,
sls_due_dt	DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date TIMESTAMP DEFAULT now()

	
);
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
CID	VARCHAR(50),
BDATE DATE,	
GEN VARCHAR(50),
dwh_create_date TIMESTAMP DEFAULT now()
);

DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
CID VARCHAR(50),
CNTRY VARCHAR(50),
dwh_create_date TIMESTAMP DEFAULT now()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
ID VARCHAR(50),
CAT VARCHAR(50),
SUBCAT	VARCHAR(50),
MAINTENANCE VARCHAR(50),
dwh_create_date TIMESTAMP DEFAULT now()
	
);

/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS
$$

BEGIN


		RAISE NOTICE '----------------------------';
		RAISE NOTICE 'Loading silver Layer';
		RAISE NOTICE '----------------------------';
	
	
		RAISE NOTICE'+++++++++++++++++++++++++++++';
		RAISE NOTICE'Loading CRM tables';
		RAISE NOTICE'+++++++++++++++++++++++++++++';


						TRUNCATE TABLE silver.crm_cust_info;
						INSERT INTO silver.crm_cust_info(
								cst_id,
								cst_key,
								cst_firstname,
								cst_lastname,
								cst_marital_status,
								cst_gndr,
								cst_create_date)
						SELECT 
						cst_id,
						cst_key,
						TRIM(cst_firstname) AS cst_firstname,
						TRIM(cst_lastname) AS cst_lastname,
						CASE 
							WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
							WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
							ELSE 'n/a'
							END as cst_marital_status,
						CASE 
							WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
							WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Male'
							ELSE 'n/a'
							END as cst_gndr,
						cst_create_date
						FROM(
							SELECT *,
							ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) RNK
							FROM bronze.crm_cust_info) T
						WHERE RNK = 1 	;
								
						
						----INSERTING PRD_INFO
						TRUNCATE TABLE silver.crm_prd_info;
						INSERT INTO silver.crm_prd_info(
										prd_id,
										cat_id,
										prd_key,
										prd_nm,
										prd_cost,
										prd_line,
										prd_start_dt,
										prd_end_dt
										
										)
						SELECT 
						prd_id,
						REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
						SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
						prd_nm,
						COALESCE(prd_cost,0) AS prd_cost,
						CASE 
							WHEN UPPER(TRIM(prd_line)) ='M' THEN 'Mountain'
							WHEN UPPER(TRIM(prd_line)) ='R' THEN 'Road'
							WHEN UPPER(TRIM(prd_line)) ='S' THEN 'Other Sales'
							WHEN UPPER(TRIM(prd_line)) ='T' THEN 'Touring'
							ELSE 'n/a'
							END AS prd_line,
						CAST(prd_start_dt AS DATE) AS prd_start_dt,
						CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt )- INTERVAL '1 day' AS DATE)  AS prd_end_dt
						FROM bronze.crm_prd_info;
						
						
						
						----INSERTING sales_details
						TRUNCATE TABLE silver.crm_sales_details;
						INSERT INTO silver.crm_sales_details (
								sls_ord_num,
								sls_prd_key,
								sls_cust_id,
								sls_order_dt,
								sls_ship_dt,
								sls_due_dt,
								sls_sales,
								sls_quantity,
								sls_price
						
											)
						select 
						sls_ord_num,
						sls_prd_key,
						sls_cust_id,
						CASE 
							WHEN sls_order_dt <= 0 OR length(cast(sls_order_dt as varchar(50)))  < 8 THEN NULL
							ELSE CAST(CAST(sls_order_dt AS varchar(50)) AS DATE) 
							END AS sls_order_dt,
						CASE 
							WHEN sls_ship_dt <= 0 OR length(cast(sls_ship_dt as varchar(50)))  < 8 THEN NULL
							ELSE CAST(CAST(sls_ship_dt AS varchar(50)) AS DATE) 
							END AS sls_ship_dt,
						CASE 
							WHEN sls_due_dt <= 0 OR length(cast(sls_due_dt as varchar(50)))  < 8 THEN NULL
							ELSE CAST(CAST(sls_due_dt AS varchar(50)) AS DATE) 
							END AS sls_due_dt,
						CASE 
							WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> sls_quantity * ABS(sls_price)
							THEN sls_quantity * ABS(sls_price)
							ELSE sls_sales
							END AS sls_sales,
						sls_quantity,
						CASE 
							WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0) 
							ELSE sls_price
							END AS sls_price
						from bronze.crm_sales_details;


		RAISE NOTICE'+++++++++++++++++++++++++++++';
		RAISE NOTICE'Loading ERP tables';
		RAISE NOTICE'+++++++++++++++++++++++++++++';
	

						--------INSERTING erp_cust_az12
						TRUNCATE TABLE silver.erp_cust_az12;
						INSERT INTO silver.erp_cust_az12(
									cid,
									bdate,
									gen
						
										)
						select 
						CASE 
							WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,length(cid))
							ELSE cid
							END  AS cid	,
						CASE 
							WHEN bdate > NOW() THEN NULL
							ELSE bdate
							END AS bdate,
						CASE 
							WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
							WHEN UPPER(TRIM(gen)) IN ('M','MALE')THEN 'Male'
							ELSE 'n/a'
							END as gen
						from bronze.erp_cust_az12;
						
						--------INSERTING erp_loc_a101
						TRUNCATE TABLE silver.erp_loc_a101;
						INSERT INTO silver.erp_loc_a101(
										cid,
										cntry
										
										)
						select 
						REPLACE(cid, '-','') AS cid,
						CASE 
							WHEN TRIM(cntry) IN('US','USA') THEN 'United States'
							WHEN TRIM(cntry) = 'DE'    THEN 'Germany'
							WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
							ELSE TRIM(cntry)
						END AS cntry
						from bronze.erp_loc_a101;
						
						
						
						-----inserting erp_px_cat_g1v2
							TRUNCATE TABLE silver.erp_px_cat_g1v2;
						INSERT INTO silver.erp_px_cat_g1v2(
									id, 
									cat,
									subcat,
									maintenance
									
									)
						
						SELECT id, cat, subcat, maintenance
						from bronze. erp_px_cat_g1v2;

END;
$$

/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
select 
ROW_NUMBER() OVER( ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE 
	WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
	ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
select 
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date

from silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
WHERE prd_end_dt IS NULL

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
select 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM 
silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
on sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id

