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

def _clean_column_names(df):
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    return df

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

def execute_transform(raw_table_name, clean_table_name):
    print(f"[Transform] Loading raw data.")
    engine = get_db_connection()

    try:
        sql_query = f'SELECT * FROM "{raw_table_name}"'
        df = pd.read_sql(sql_query, engine)
    except Exception as e:
        raise Exception(f"Failed to extract data {raw_table_name}: {e}")

    df = _clean_column_names(df)
    df['income'] = pd.to_numeric(df['income'], errors='coerce').fillna(0).astype(int)
    
    bins = [0, 18, 30, 50, 70, 100]
    labels = ['Teen', 'Young Adult', 'Middle Aged', 'Senior', 'Elderly']
    df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels, right=False)
    
    binary_cols = ['history_of_mental_illness', 'history_of_substance_abuse', 
                   'family_history_of_depression', 'chronic_medical_conditions']
    
    for col in binary_cols:
        if col in df.columns:
            df[col] = df[col].map({'Yes': 1, 'No': 0}).fillna(0).astype(int)

    print("[Transform] Calculating stats.")
    df_stats = df.groupby(['age_group', 'marital_status'], dropna=False).agg(
        group_total_records=('name', 'count'),
        group_avg_income=('income', 'mean')
    ).reset_index()

    print("[Transform] Merging stats.")
    df_final = pd.merge(df, df_stats, on=['age_group', 'marital_status'], how='left')

    try:
        print(f"[Transform] Saving {len(df_final)} rows to DB.")
        df_final.to_sql(
            clean_table_name, 
            engine, 
            if_exists='replace',
            index=False, 
            method=psql_insert_copy
        )
        print(f"[Transform] Saved successfully.")
    except Exception as e:
        raise Exception(f"Failed to load data {clean_table_name}: {e}")