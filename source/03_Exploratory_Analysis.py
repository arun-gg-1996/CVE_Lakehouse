# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: CVE Exploratory Data Analysis
# MAGIC
# MAGIC This notebook performs the final analysis on our clean, normalized Silver-layer tables. The goal is to derive actionable business intelligence and identify key trends in the 2024 cybersecurity vulnerability data.
# MAGIC
# MAGIC Each query below represents a "Gold" analysis, ready for reporting or visualization.

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, desc, month, year, to_date, trunc

# Load our clean Silver tables into DataFrames
try:
    silver_cves_df = spark.table("cve_silver.cves")
    silver_products_df = spark.table("cve_silver.affected_products")

    print("Successfully loaded and cached Silver tables.")
    print("\nSample of CVEs table:")
    silver_cves_df.show(5, truncate=False)
    print("\nSample of Affected Products table:")
    silver_products_df.show(5, truncate=False)

except Exception as e:
    print(f"AN ERROR OCCURRED: Make sure you have successfully run the Silver Layer notebook. Error: {e}")

# COMMAND ----------

# Gold Layer Analysis #1
# Objective: Identify which vendors have the most vulnerabilities.

print("--- Gold Analysis 1: Top 25 Vendors by Vulnerability Count ---")

top_vendors_df = (silver_products_df
    # Filter out null or generic vendor names for a cleaner analysis
    .filter(col("vendor").isNotNull() & ~col("vendor").isin("n/a", "N/A"))
    .groupBy("vendor")
    .agg(countDistinct("cve_id").alias("vulnerability_count"))
    .orderBy(desc("vulnerability_count"))
    .limit(25)
)

display(top_vendors_df)

# COMMAND ----------

# Gold Layer Analysis #2
# Objective: Understand the overall risk profile of the 2024 vulnerabilities.

print("--- Gold Analysis 2: Vulnerability Severity Distribution ---")

severity_distribution_df = (silver_cves_df
    .filter(col("cvss_severity").isNotNull())
    .groupBy("cvss_severity")
    .count()
    .orderBy(desc("count"))
)

display(severity_distribution_df)

# COMMAND ----------

# Gold Layer Analysis #3
# Objective: Analyze the trend of vulnerability disclosures over time in 2024.

print("--- Gold Analysis 3: Vulnerability Disclosures Per Month ---")

monthly_trends_df = (silver_cves_df
    # Create a new column 'publication_month' by truncating the date to the month
    .withColumn("publication_month", trunc(col("date_published"), "MM"))
    .groupBy("publication_month")
    .count()
    .orderBy("publication_month")
)

display(monthly_trends_df)

# COMMAND ----------

# Gold Layer Analysis #4
# Objective: Identify specific products most frequently associated with vulnerabilities.

print("--- Gold Analysis 4: Top 10 Most Vulnerable Products ---")

# Define a list of generic/null values to filter out
filter_list = ["n/a", "N/A", "Unknown"]

top_products_df = (silver_products_df
    # Filter out nulls and generic vendor/product names for a cleaner analysis
    .filter(col("vendor").isNotNull() & ~col("vendor").isin(filter_list))
    .filter(col("product").isNotNull() & ~col("product").isin(filter_list))
    .groupBy("vendor", "product")
    .agg(countDistinct("cve_id").alias("vulnerability_count"))
    .orderBy(desc("vulnerability_count"))
    .limit(10)
)

display(top_products_df)