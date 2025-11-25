import sys
import time
import traceback

try:
    from ingest import execute_ingest
    from transform import execute_transform
    from publish import extract_clean_data, execute_publish
except ImportError as e:
    print(f"Import failed. {e}")
    sys.exit(1)

CSV_FILE = "/files/depression_data.csv"
RAW_TABLE = "raw_data"
CLEAN_TABLE = "clean_data"

SPREADSHEET = "Depression Report"
WORKSHEET = "Summary Data"
SERVICE_ACCOUNT_KEY_FILENAME = "/files/service-account-key.json" 

def run_full_pipeline_debug():
    print("=== PIPELINE STARTED ===")
    
    print("--- 1. Ingesting ---")
    execute_ingest(CSV_FILE, RAW_TABLE)
    
    print("--- 2. Transforming ---")
    execute_transform(RAW_TABLE, CLEAN_TABLE)

    print("--- 3. Publishing ---")
    final_df = extract_clean_data(CLEAN_TABLE)
    execute_publish(final_df, SPREADSHEET, WORKSHEET, SERVICE_ACCOUNT_KEY_FILENAME)
    
    print("=== PIPELINE SUCCESS ===")

if __name__ == "__main__":
    try:
        print("Waiting for database...")
        time.sleep(5)
        run_full_pipeline_debug()
    except Exception:
        print("--- PIPELINE FAILED ---")
        traceback.print_exc() 
        sys.exit(1)