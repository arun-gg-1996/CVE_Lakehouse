import os
import pandas as pd
from tqdm import tqdm
import glob
from multiprocessing import Pool, cpu_count
import pyarrow as pa
import pyarrow.parquet as pq
import random

# --- 1. Configuration ---
# The root directory you want to replicate EXACTLY
root_path = 'cvelistV5'
# The output file
output_parquet_file = 'filesystem_snapshot.parquet'
BATCH_SIZE = 5000
# Number of random files to test for content integrity
VERIFICATION_SAMPLE_SIZE = 100


# --- 2. Generic Worker Function ---
def process_file(file_path):
    try:
        # Calculate the file's path relative to the root directory
        relative_path = os.path.relpath(file_path, root_path)

        # Read the file's raw binary content
        with open(file_path, 'rb') as f:
            file_content = f.read()

        return {
            'relative_path': relative_path,
            'file_content': file_content
        }
    except Exception:
        return None


# --- 3. Main Script Execution ---
if __name__ == "__main__":
    print(f"Scanning all files recursively inside: {root_path}")
    all_files = [f for f in glob.glob(os.path.join(root_path, '**'), recursive=True) if os.path.isfile(f)]
    total_files = len(all_files)
    print(f"Found {total_files} total files to consolidate.")

    # --- Data Consolidation ---
    pool = Pool(cpu_count())
    parquet_writer = None
    batch = []

    results_iterator = pool.imap_unordered(process_file, all_files)

    print(f"Starting parallel consolidation with {cpu_count()} CPU cores...")
    for record in tqdm(results_iterator, total=total_files, desc="Consolidating filesystem"):
        if record:
            batch.append(record)

        if len(batch) >= BATCH_SIZE:
            df_batch = pd.DataFrame(batch)
            table_batch = pa.Table.from_pandas(df_batch)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(output_parquet_file, table_batch.schema, compression='gzip')
            parquet_writer.write_table(table_batch)
            batch = []

    if batch:
        df_batch = pd.DataFrame(batch)
        table_batch = pa.Table.from_pandas(df_batch)
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(output_parquet_file, table_batch.schema, compression='gzip')
        parquet_writer.write_table(table_batch)

    if parquet_writer:
        parquet_writer.close()

    pool.close()
    pool.join()

    print(f"\nConsolidation complete! Output file: {output_parquet_file}")

    # --- 4. COMPREHENSIVE VERIFICATION STEP ---
    print("\n--- Verifying the output Parquet file ---")
    try:
        table = pq.read_table(output_parquet_file)
        df_verify = table.to_pandas()
        actual_rows = len(df_verify)
        print(f"Successfully read the Parquet file. Total files consolidated: {actual_rows}")

        # Test 1: Row Count Integrity
        if actual_rows == total_files:
            print(f"✅ TEST PASSED: Row count ({actual_rows}) matches the number of files found ({total_files}).")
        else:
            print(f"❌ TEST FAILED: Row count ({actual_rows}) does not match files found ({total_files}).")
            exit()  # Stop if counts don't match

        # Test 2: Content Integrity (Random Sampling)
        print(f"\n--- Performing content verification on {VERIFICATION_SAMPLE_SIZE} random files ---")

        # Get a random sample of rows from the DataFrame
        # Ensure we don't sample more files than exist
        sample_size = min(VERIFICATION_SAMPLE_SIZE, actual_rows)
        random_sample = df_verify.sample(n=sample_size)

        mismatched_files = 0
        for index, row in tqdm(random_sample.iterrows(), total=sample_size, desc="Verifying content"):
            # Get the data from our Parquet file
            relative_path_from_parquet = row['relative_path']
            content_from_parquet = row['file_content']

            # Reconstruct the original file path on the local disk
            original_file_path = os.path.join(root_path, relative_path_from_parquet)

            # Read the original file's content
            try:
                with open(original_file_path, 'rb') as f:
                    content_from_disk = f.read()

                # Compare the content
                if content_from_parquet != content_from_disk:
                    print(f"\nERROR: Content mismatch for file: {original_file_path}")
                    mismatched_files += 1
            except FileNotFoundError:
                print(f"\nERROR: Original file not found for path: {original_file_path}")
                mismatched_files += 1

        if mismatched_files == 0:
            print(f"\n✅ TEST PASSED: Successfully verified content of {sample_size} out of {sample_size} random files.")
        else:
            print(f"\n❌ TEST FAILED: Found {mismatched_files} content mismatches.")

        print("\n--- Verification Complete. The Parquet file is ready for upload if all tests passed. ---")

    except Exception as e:
        print(f"\nAn error occurred during verification: {e}")
