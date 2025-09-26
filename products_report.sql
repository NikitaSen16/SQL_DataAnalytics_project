/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================

DROP VIEW IF EXISTS gold.report_products;
CREATE VIEW  gold.report_products AS
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
--Product Aggregations: Summarizes key metrics at the product level
product_aggregation AS(

				SELECT 
				product_key,
				product_name, 
				category,
				subcategory,
				cost,
				EXTRACT(YEAR FROM AGE(max(order_date), min(order_date))) * 12 + 
				EXTRACT(MONTH FROM AGE(max(order_date), min(order_date))) AS lifespan,
				max(order_date) as last_sale_date,
				count(DISTINCT order_number) as total_orders,
				count(distinct customer_key) As total_customers,
				sum(sales_amount) AS total_sales,
				sum(quantity) as total_quantity,
				AVG(Cast(sales_amount AS float)/nullif(quantity,0)) AS avg_selling_price
				FROM base_query
				GROUP BY 
				product_key,
				product_name, 
				category,
				subcategory,
				cost
				)

-- Final Query: Combines all product results into one output

SELECT
product_key,
product_name, 
category,
subcategory,
cost,
last_sale_date,
EXTRACT(MONTH FROM age(current_date,last_sale_date)) AS recency_in_months,
CASE 
	WHEN total_sales > 50000 THEN 'High-Performer'
	WHEN total_sales >=10000 THEN 'Mid-Range'
	ELSE 'Low_performer'
END as product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
total_customers,
avg_selling_price,
CASE 
	WHEN total_orders = 0 THEN 0 
	ELSE total_sales/total_orders 
END AS avg_order_revenue,
CASE 
	WHEN lifespan = 0 THEN total_sales
	ELSE total_sales/lifespan
END AS avg_monthly_revenue
FROM product_aggregation




