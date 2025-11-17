-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer: CVE Exploratory Data Analysis
-- MAGIC
-- MAGIC This notebook performs the final analysis on our clean, normalized Silver-layer tables. The goal is to derive actionable business intelligence and identify key trends in the 2024 cybersecurity vulnerability data.
-- MAGIC
-- MAGIC Each query below represents a "Gold" analysis, ready for reporting or visualization.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 0. Verify Tables

-- COMMAND ----------

SELECT 'CVEs Table' AS table_name, COUNT(*) AS record_count FROM cve_silver.cves
UNION ALL
SELECT 'Affected Products Table', COUNT(*) FROM cve_silver.affected_products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Top 25 Vendors

-- COMMAND ----------

-- Top 25 Vendors by Vulnerability Count
SELECT 
    vendor,
    COUNT(DISTINCT cve_id) AS vulnerability_count
FROM cve_silver.affected_products
WHERE vendor IS NOT NULL 
    AND vendor NOT IN ('n/a', 'N/A')
GROUP BY vendor
ORDER BY vulnerability_count DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Severity Distribution

-- COMMAND ----------

-- Vulnerability Severity Distribution
SELECT 
    cvss_severity,
    COUNT(*) AS count
FROM cve_silver.cves
WHERE cvss_severity IS NOT NULL
GROUP BY cvss_severity
ORDER BY count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Monthly Trends

-- COMMAND ----------

-- Vulnerability Disclosures Per Month
SELECT 
    DATE_TRUNC('MONTH', date_published) AS publication_month,
    COUNT(*) AS count
FROM cve_silver.cves
GROUP BY publication_month
ORDER BY publication_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Top 10 Products

-- COMMAND ----------

-- Top 10 Most Vulnerable Products
SELECT 
    vendor,
    product,
    COUNT(DISTINCT cve_id) AS vulnerability_count
FROM cve_silver.affected_products
WHERE vendor IS NOT NULL 
    AND vendor NOT IN ('n/a', 'N/A', 'Unknown')
    AND product IS NOT NULL 
    AND product NOT IN ('n/a', 'N/A', 'Unknown')
GROUP BY vendor, product
ORDER BY vulnerability_count DESC
LIMIT 10;
