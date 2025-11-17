**Course:** EAS 587 - Data Intensive Computing  
**Date:** Spring 2025  
**Student Details:** Arun Ghontale (#50625471)
-----------
# CVE Vulnerability Intelligence Platform - 2024

A data lakehouse analyzing 40,000+ cybersecurity vulnerabilities from 2024 using Databricks and the Medallion Architecture (Bronze → Silver → Gold).

## About

Processes raw CVE (Common Vulnerabilities and Exposures) data to identify which tech vendors and products have the most security issues. Built using real-world data engineering patterns on Databricks.

## Structure

### 📂 `source/` - All Notebooks

**Data Setup:**
- `create_parquet.py` - Local Python script that packages the entire CVE GitHub repository into a single Parquet file for upload to Databricks
- `00_Data_Download.py` - Uploads the Parquet file to Databricks and recreates the folder structure (317k+ files)
- `00_File_Creation_Check.py` - Monitors the file extraction progress

**Bronze Layer (Raw Data Ingestion):**
- `01_Bronze_Ingestion.py` - Reads all JSON files, filters to 2024 CVEs only (~40k records), creates Delta table `cve_bronze.records`

**Silver Layer (Data Cleaning):**
- `02_Silver_Normalization.py` - Transforms nested JSON into two clean tables:
  - `cve_silver.cves` - One row per vulnerability with scores and descriptions
  - `cve_silver.affected_products` - Exploded vendor/product relationships

**Gold Layer (Analysis):**
- `03_Exploratory_Analysis.py` - Python version (exploratory work)
- `03_Exploratory_Analysis_sql.sql` - **Final SQL analysis** with 4 key insights:
  1. Top 25 vendors by vulnerability count
  2. Risk severity distribution
  3. Monthly disclosure trends
  4. Top 10 most vulnerable products

### `plots/` - Visualizations

Generated from SQL queries in Databricks:
- `vendor_vulnerability.png` - Linux dominates with 5000+ CVEs
- `risk_distribution.png` - Most vulnerabilities are MEDIUM severity
- `vulnerability_per_month.png` - Huge spike in May 2024
- `products_with_most_vulnarabilities.png` - Top affected products

### `screenshots/` - Proof of Work

PDFs showing successful execution of each layer with data quality checks passed.

## How to Re-run this

1. **Downloaded** the entire CVE repository from GitHub (~317k JSON files)
2. **Packaged** it into a single Parquet file for efficient upload
3. **Ingested** to Databricks and filtered to 2024 data only
4. **Normalized** nested JSON into queryable relational tables
5. **Analyzed** using SQL to find security trends and risk patterns

## Key Findings from 2024 Data

- **Linux** had the most vulnerabilities (5000+), followed by Microsoft
- **May 2024** saw a massive spike in disclosures (5000+ CVEs)
- Most vulnerabilities are rated **MEDIUM** severity (~14k)
- **2024 was a record-breaking year** for vulnerability disclosures

## Running This Project

1. Upload Parquet file to Databricks Unity Catalog volume
2. Run notebooks in order: `00 → 01 → 02 → 03`
3. Each layer builds on the previous (Bronze → Silver → Gold)
4. SQL analysis notebook generates all visualizations
