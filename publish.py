import os
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import gspread 
from gspread.exceptions import APIError, WorksheetNotFound 
from gspread.utils import rowcol_to_a1

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def get_db_connection():
    try:
        connection_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_str)
        return engine
    except Exception:
        raise

def extract_clean_data(clean_table_name):
    engine = get_db_connection()
    try:
        print(f"[Publish] Reading data from DB table: {clean_table_name}.")
        sql_query = f"SELECT * FROM {clean_table_name}"
        df = pd.read_sql(sql_query, engine)
        return df
    except Exception as e:
        raise Exception(f"Failed to extract data from {clean_table_name}: {e}")

def execute_publish(df, spreadsheet_name, worksheet_name, key_filename):
    total_rows = len(df)
    total_cols = len(df.columns)
    print(f"[Publish] Processing {total_rows} rows x {total_cols} cols.")
    
    if not os.path.exists(key_filename):
        raise FileNotFoundError(f"Service Account Key file not found: {key_filename}")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna('') 

    try:
        gc = gspread.service_account(filename=key_filename) 
        spreadsheet = gc.open(spreadsheet_name)
        
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            print(f"[Publish] Found existing worksheet. Resetting size.")

            worksheet.resize(rows=1, cols=1)
            worksheet.clear()

            worksheet.resize(rows=total_rows + 1, cols=total_cols)
            
        except WorksheetNotFound:
            print(f"[Publish] Creating new worksheet.")
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name, 
                rows=total_rows + 1, 
                cols=total_cols
            )

        all_values = [df.columns.values.tolist()] + df.values.tolist()
        
        batch_size = 30000
        total_batches = (len(all_values) // batch_size) + 1
        
        print(f"[Publish] Uploading in {total_batches} batches.")

        for i in range(total_batches):
            start_row_idx = i * batch_size
            end_row_idx = min((i + 1) * batch_size, len(all_values))
            
            if start_row_idx >= end_row_idx:
                break

            chunk_data = all_values[start_row_idx:end_row_idx]

            start_cell = rowcol_to_a1(start_row_idx + 1, 1)
            end_cell = rowcol_to_a1(end_row_idx, total_cols)
            range_name = f"{start_cell}:{end_cell}"

            print(f"[Publish] Uploading Batch {i+1}/{total_batches} ({len(chunk_data)} rows).")

            worksheet.update(range_name, chunk_data)
            time.sleep(2) 

        print("[Publish] Upload successfully completed.")
        
    except APIError as e:
        raise Exception(f"Google Sheet API Error: {e}")
    except Exception as e:
        raise Exception(f"Error Google Sheet upload: {e}")