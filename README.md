# 🚲 Bike Sales Analysis

This project demonstrates the creation of a full data warehouse and analytics pipeline using **PostgreSQL**, 
based on the **bronze-silver-gold** layered architecture. 
The dataset used is related to **bike sales**, including customer information, products information, and sales data.

---

## 📁 Project Structure

### 1. **Database Setup**
- Created a PostgreSQL database
- Defined 3 main schemas:
  - **bronze**: raw data
  - **silver**: cleaned and enriched data
  - **gold**: final analytical views and dimensional model

---

## 🛠️ Data Engineering Workflow

### 🥉 Bronze Layer (Raw Ingestion)
- Loaded all original, unprocessed datasets into tables in the `bronze` schema.
- This layer acts as a **data lake**, mirroring the raw source data.
- No transformation applied here — just extraction.
- - 🧩 **Stored Procedure** created to automate raw data insertion.

### 🥈 Silver Layer (Data Cleaning & Enrichment)
- Cleaned and transformed data from `bronze`, including:
  - Removing nulls, fixing data types, standardizing values
  - Joining related tables
- Inserted clean and structured data into the `silver` schema.
- Bronze data remained untouched to ensure data traceability.

### 🥇 Gold Layer (Star Schema & Views)
- Created **dimension and fact views** from `silver` data, following a **star schema** model.
- Views include:
  - `dim_customer`
  - `dim_product`
  - `fact_sales`
- These views represent the **semantic layer** used for analysis and reporting.
- - 🛠️ **Stored Procedure** created to automate data transformation and loading into `silver`.

---

## 📊 Exploratory Data Analysis (EDA)

Performed detailed analysis on the `gold` layer:

### 🔍 EDA Activities
- Schema exploration and data profiling
- Dimension-wise and date-wise sales analysis
- Measure statistics and magnitude comparisons
- Ranking customers/products by performance

---

## 📈 Advanced Analytics

Went beyond EDA with analytical SQL techniques such as:

- **Trend Analysis**: Year-over-year and month-over-month sales trends
- **Cumulative Analysis**: Running totals for sales and quantities
- **Performance Analysis**: Customer/product performance over time
- **Part-to-Whole Analysis**: Category-wise contribution to overall sales
- **Data Segmentation**: Customer age/gender segmentation

---

## 📄 Final Reports

Created **2 key analytical views**:
- `customer_report_view`: all major KPIs and trends related to customers
- `product_report_view`: performance of products by category and subcategory

These views are designed to feed into BI tools like **Power BI** or **Tableau** for reporting.


## 🧠 Tools & Tech Stack

- **PostgreSQL** for database and SQL transformations
- **SQL** (CTEs, window functions, aggregation, joins)

---

## ✅ Key Learnings

- Implemented a modern **data warehouse layer architecture**
- Applied real-world **ETL logic** using only SQL
- Gained deep insights from structured **bike sales data**
- Prepared the data model for easy integration with BI tools

---## 📈 Key KPIs (Sample)

| Metric                                | Value                          |
|---------------------------------------|---------------------------------|
| 🛒 **Total Sales Revenue**            | $29,356,250                    |
| 👥 **Total Unique Customers**         | 18,485                         |
| 📦 **Total Quantity Sold**            | 60,423 units                   |
| 🧾 **Total Number of Products**       | 295                            |
| 🚲 **Best-Selling Category**          | Bikes                          |
| 🛍️ **Best-Selling Subcategory**       | Road Bikes                     |
| 🌍 **Top Performing Country**         | United States                  |
| 💰 **Highest Spending Customer**      | Nichole Nara ($13,294)         |
| ⭐ **Top Revenue-Generating Product**  | Mountain-200 Black - 46       |

<img width="1320" height="738" alt="Screenshot 2025-09-29 143504" src="https://github.com/user-attachments/assets/aba05004-6ade-4418-b718-1fa3a1568c6c" />


