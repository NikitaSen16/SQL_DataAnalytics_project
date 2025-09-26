/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================

DROP VIEW IF EXISTS gold.report_customers;
CREATE VIEW gold.report_customers AS
-- base query: core columns from table

WITH base_query AS(
				SELECT 
				s.order_number,
				s.product_key,
				s.order_date,
				s.sales_amount,
				s.quantity,
				c.customer_key,
				c.customer_number,
				c.first_name||' '||c.last_name as customer_name,
				EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) as age
				FROM gold.fact_sales s
				LEFT JOIN gold.dim_customers c
				on s.customer_key = c.customer_key
				WHERE order_date IS NOT NULL
				),
--Customer Aggregations: Summarizes key metrics at the customer level

customer_aggregation AS(
				SELECT 
				customer_key,
				customer_number,
				customer_name,
				age,
				count(DISTINCT order_number) AS total_orders,
				sum(sales_amount) AS total_sales,
				sum(quantity) AS total_quantity,
				count(Distinct Product_key) AS total_products,
				max(order_date) AS last_order_date,
				EXTRACT(YEAR FROM AGE(max(order_date), min(order_date))) * 12 + 
				EXTRACT(MONTH FROM AGE(max(order_date), min(order_date))) AS lifespan
				FROM base_query
				GROUP BY 
				customer_key,
				customer_number,
				customer_name,
				age
				)


SELECT 
customer_key,
customer_number,
customer_name,
age,
CASE
	WHEN age < 20 THEN 'Under 20'
	WHEN age BETWEEN 20 AND 29 THEN '20-29'
	WHEN age BETWEEN 30 AND 39 THEN '30-39'
	WHEN age BETWEEN 40 AND 49 THEN '40-49'
	ELSE '50 and above'
END AS age_group,
CASE 
	WHEN lifespan >=12 AND total_sales > 5000 THEN 'VIP'
	WHEN lifespan >= 12 AND total_sales <=5000 THEN 'Regular'
	ELSE 'New'
END as customer_segment,
last_order_date,
EXTRACT(YEAR FROM AGE(CURRENT_DATE, last_order_date)) * 12 + 
EXTRACT(MONTH FROM  AGE(CURRENT_DATE, last_order_date)) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
CASE 
	WHEN total_orders = 0 THEN 0 
	ELSE total_sales/total_orders
END AS average_order_value,
CASE 
	WHEN lifespan = 0 THEN total_sales
	ELSE total_sales / lifespan
END as Average_monthly_spend
FROM 
customer_aggregation
