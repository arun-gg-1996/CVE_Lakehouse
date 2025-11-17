# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Cleaning, Normalization and Validation

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, explode_outer, to_timestamp

# --- SETUP ---
# Create the target schema for Silver tables if it doesn't already exist.
print("Ensuring schema 'cve_silver' exists...")
spark.sql("CREATE SCHEMA IF NOT EXISTS cve_silver")
print("Schema setup complete.")

# --- LOAD DATA ---
# Load the Bronze table that was created in the bronze notebook.
bronze_df = spark.table("cve_bronze.records")


print("\nSuccessfully loaded and cached the Bronze data.")
display(bronze_df.limit(5))

# COMMAND ----------

# --- 1. Create the Core CVE Table ---

# Select and flatten the necessary fields from the Bronze DataFrame.
silver_cves_df = bronze_df.select(
    col("cveMetadata.cveId").alias("cve_id"),
    # Standardize the date string to a proper timestamp format
    to_timestamp(col("cveMetadata.datePublished")).alias("date_published"),
    col("containers.cna.title").alias("title"),
    # Extract the first description from the descriptions array
    col("containers.cna.descriptions")[0]["value"].alias("description"),
    # Use coalesce to find the first non-null score, searching in order of preference (v4.0, then v3.1)
    coalesce(
        col("containers.cna.metrics")[0]["cvssV4_0"]["baseScore"],
        col("containers.cna.metrics")[0]["cvssV3_1"]["baseScore"]
    ).alias("cvss_score"),
    coalesce(
        col("containers.cna.metrics")[0]["cvssV4_0"]["baseSeverity"],
        col("containers.cna.metrics")[0]["cvssV3_1"]["baseSeverity"]
    ).alias("cvss_severity")
)

# Write the clean, flattened data to a new Silver table.
(silver_cves_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("cve_silver.cves")
)

print("Successfully created the 'cve_silver.cves' table.")
display(spark.table("cve_silver.cves"))

# COMMAND ----------

# --- 2. Create the Affected Products Table ---

# Explode the 'affected' array to create a one-to-many relationship.
silver_affected_products_df = bronze_df.select(
    col("cveMetadata.cveId").alias("cve_id"),
    explode_outer(col("containers.cna.affected")).alias("affected_product")
).select(
    "cve_id",
    col("affected_product.vendor").alias("vendor"),
    col("affected_product.product").alias("product")
)

# Write the exploded data to its own Silver table.
(silver_affected_products_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("cve_silver.affected_products")
)

print("Successfully created the 'cve_silver.affected_products' table.")
display(spark.table("cve_silver.affected_products"))

# COMMAND ----------

# --- 3. Data Quality Checks on the Silver Table ---
print("--- Running Data Quality Checks on the 'cve_silver.cves' Table ---")

# Load the newly created Silver table for verification.
final_silver_table = spark.table("cve_silver.cves")

# 1. Row Count Check (matches the Bronze layer requirement)
total_count = final_silver_table.count()
print(f"Total 2024 CVEs loaded into Silver table: {total_count}")
assert total_count >= 30000, f"DATA QUALITY FAILED: Expected >= 30,000 records, found {total_count}."
print("✅ Quality Check Passed: Record count is above threshold.")

# 2. Null ID Check
null_id_count = final_silver_table.filter(col("cve_id").isNull()).count()
print(f"Number of records with null cve_id: {null_id_count}")
assert null_id_count == 0, "DATA QUALITY FAILED: Found records with null cve_id."
print("✅ Quality Check Passed: No null CVE IDs found.")

# 3. Uniqueness Check
distinct_id_count = final_silver_table.select("cve_id").distinct().count()
print(f"Total count: {total_count} | Distinct CVE IDs: {distinct_id_count}")
assert total_count == distinct_id_count, "DATA QUALITY FAILED: Found duplicate CVE IDs."
print("✅ Quality Check Passed: All CVE IDs are unique.")

print("\n--- All data quality checks passed! Silver layer is complete. ---")