import os
import pandas as pd
import csv
from io import StringIO
from sqlalchemy import create_engine
from dotenv import load_dotenv

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
        raise Exception("Database connection failed")

def psql_insert_copy(table, conn, keys, data_iter):
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def execute_ingest(file_path, table_name):
    print(f"[Ingest] Reading file: {file_path}")
    engine = get_db_connection()
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")

    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"[Ingest] CSV loaded. Rows: {len(df)}")

        df.to_sql(
            table_name, 
            engine, 
            if_exists='replace',
            index=False, 
            method=psql_insert_copy
        )
        print(f"[Ingest] Success! {len(df)} rows ingested instantly.")
        
    except Exception as e:
        raise Exception(f"Failed to ingest data {table_name}: {e}")