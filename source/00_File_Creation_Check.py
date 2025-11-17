# Databricks notebook source
import os
import time
from datetime import datetime

# --- 1. Configuration ---
# This MUST be the exact same path your main script is writing to.
output_root_path = "/Volumes/workspace/default/assignment1/recreated_cvelistV5"

# This is the total number of files you expect. 
# We get this from the output of your main script.
total_files_expected = 317623 

# How often to check for updates, in seconds.
check_interval_seconds = 30

print(f"--- Starting Progress Monitor ---")
print(f"Target Directory: {output_root_path}")
print(f"Expected Total Files: {total_files_expected}")
print(f"Checking every {check_interval_seconds} seconds. Press the 'Interrupt' button to stop.")
print("--------------------------------------------------")

# --- 2. Monitoring Loop ---
try:
    # This loop will run forever until you manually stop it.
    while True:
        # Get the current time for the log entry
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Check if the target directory has been created yet
        if not os.path.exists(output_root_path):
            print(f"[{timestamp}] - Waiting for target directory to be created...")
            time.sleep(check_interval_seconds)
            continue # Skip to the next loop iteration

        # os.walk is a Python function that "walks" through a directory tree.
        # We use it here to count every file in the nested structure.
        current_file_count = 0
        for dirpath, dirnames, filenames in os.walk(output_root_path):
            current_file_count += len(filenames)
            
        # Calculate the progress percentage
        if total_files_expected > 0:
            progress_percent = (current_file_count / total_files_expected) * 100
        else:
            progress_percent = 0
            
        # Print the formatted status update
        print(f"[{timestamp}] - Progress: {current_file_count} / {total_files_expected} files created ({progress_percent:.2f}%)")
        
        # If all files are created, stop the loop.
        if current_file_count >= total_files_expected:
            print("\n--- Monitoring Complete: All expected files have been created. ---")
            break
            
        # Wait for the specified interval before checking again.
        time.sleep(check_interval_seconds)

except KeyboardInterrupt:
    # This allows you to stop the cell cleanly using the "Interrupt" button.
    print("\n--- Monitoring stopped by user. ---")

# COMMAND ----------

