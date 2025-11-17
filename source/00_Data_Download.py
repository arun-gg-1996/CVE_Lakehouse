# Databricks notebook source
!pip install tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/assignment1/

# COMMAND ----------

# --- Code to Show First 5 Rows ---

# Define the path to your Parquet file
parquet_path = "/Volumes/workspace/default/assignment1/filesystem_snapshot.parquet"

# Read the Parquet file into a Spark DataFrame
df = spark.read.parquet(parquet_path)

# Show the first 5 rows of the DataFrame
# truncate=False ensures you can see the full content of each column
df.show(5, truncate=False)

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql.functions import col, spark_partition_id

# --- 1. Configuration ---
parquet_path = "/Volumes/workspace/default/assignment1/filesystem_snapshot.parquet"
output_root_path = "/Volumes/workspace/default/assignment1/recreated_cvelistV5"

# --- NEW: PARALLELISM CONFIGURATION ---
# We will force Spark to use this many parallel tasks.
NUM_PARALLEL_TASKS = 200

print(f"Reading snapshot from: {parquet_path}")
print(f"Will recreate the exact folder structure inside: {output_root_path}")

# --- 2. Read and REPARTITION the DataFrame ---
df = spark.read.parquet(parquet_path)

print(f"\nIncreasing parallelism to {NUM_PARALLEL_TASKS} tasks...")

# *** THIS IS THE KEY TO MORE SPEED ***
# Repartition the DataFrame into many more pieces for higher parallelism.
df_repartitioned = df.repartition(NUM_PARALLEL_TASKS)

# --- 3. Verify Parallelism (The Correct, Serverless-Compliant Way) ---
# This runs a quick, separate job to count the distinct partition IDs.
actual_tasks = df_repartitioned.select(spark_partition_id()).distinct().count()

print("\n--- Parallelism Plan ---")
print(f"The DataFrame is now split into {actual_tasks} partitions.")
print(f"Spark will run up to {actual_tasks} tasks in parallel to process this data.")
print("--------------------------\n")

# --- 4. Define the BATCHED File Writing Function ---
# This function receives an ITERATOR of Pandas DataFrames.
def write_files_in_batch(pdf_iterator: iter) -> pd.DataFrame:
    for pdf in pdf_iterator:
        for index, row in pdf.iterrows():
            clean_relative_path = row['relative_path'].replace('\\', '/')
            full_destination_path = os.path.join(output_root_path, clean_relative_path)
            destination_dir = os.path.dirname(full_destination_path)
            os.makedirs(destination_dir, exist_ok=True)
            with open(full_destination_path, "wb") as f:
                f.write(row['file_content'])
    return pd.DataFrame()

# --- 5. Run the BLAZING FAST, REPARTITIONED Job ---
print("\nStarting the HIGHLY PARALLEL, distributed file recreation.")
print("!!! MONITOR THE SPARK UI PROGRESS BAR BELOW THIS CELL !!!")

# We call mapInPandas on the REPARTITIONED DataFrame.
df_repartitioned.mapInPandas(write_files_in_batch, schema="result INT").collect()

print("\nFile structure recreated successfully!")

# --- 6. Final Verification in Databricks ---
print("\n--- Verifying recreated structure (listing top-level items) ---")
display(dbutils.fs.ls(output_root_path))

# COMMAND ----------

