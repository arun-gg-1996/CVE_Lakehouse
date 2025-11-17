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

## Key Technologies

- **Databricks** - Cloud data platform
- **Apache Spark / PySpark** - Distributed data processing
- **Delta Lake** - ACID transactions for data lakes
- **SQL** - Business intelligence queries

## Key Findings from 2024 Data

- **Linux** had the most vulnerabilities (5000+), followed by Microsoft
- **May 2024** saw a massive spike in disclosures (5000+ CVEs)
- Most vulnerabilities are rated **MEDIUM** severity (~14k)
- **2024 was a record-breaking year** for vulnerability disclosures

---

## How to Reproduce This Project

### Required
- Databricks Community Edition account (free at https://community.cloud.databricks.com/)
- Unity Catalog volume path: `/Volumes/workspace/default/assignment1`

### Step 1: Prepare Data Locally
1. Clone CVE repository from GitHub: `git clone https://github.com/CVEProject/cvelistV5`
2. Run `create_parquet.py` on your local machine
   - This packages all 317k JSON files into a single Parquet file
   - Takes ~3-4 minutes, outputs `filesystem_snapshot.parquet`
3. Upload the Parquet file to Databricks

### Step 2: Extract Data in Databricks
1. Open `00_Data_Download.py` in Databricks
2. Run all cells - this extracts the Parquet and recreates the CVE folder structure
3. Run `00_File_Creation_Check.py` in parallel to monitor progress

### Step 3: Build Bronze Layer
1. Open `01_Bronze_Ingestion.py`
2. Run all cells - this reads 317k JSON files, filters to 2024 only, creates `cve_bronze.records` table
3. Verify: You should see ~40,000 records and data quality checks passing

### Step 4: Build Silver Layer
1. Open `02_Silver_Normalization.py`
2. Run all cells - this normalizes the Bronze data into two clean tables
3. Verify: Check that `cve_silver.cves` and `cve_silver.affected_products` tables exist

### Step 5: Run Analysis
1. Open `03_Exploratory_Analysis_sql.sql` (SQL notebook)
2. Run each query cell to generate insights
3. Click the "+" button on results to create visualizations (bar charts, line charts, pie charts)
4. Export your visualizations to the `plots/` folder

### Expected Outputs
- **Tables Created:** `cve_bronze.records`, `cve_silver.cves`, `cve_silver.affected_products`
- **Record Count:** ~40,000 CVEs from 2024
- **Visualizations:** 4 charts showing vendor rankings, severity distribution, temporal trends, and product vulnerabilities
