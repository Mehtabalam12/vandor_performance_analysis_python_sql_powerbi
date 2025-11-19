import os
import pandas as pd
import gc
import psutil
from sqlalchemy import create_engine

engine = create_engine('sqlite:///my_database.db')  

def log_usage(step=""):
    mem = psutil.virtual_memory()
    print(f"[{step}] RAM: {mem.used / 1e9:.2f} GB / {mem.total / 1e9:.2f} GB")

def ingest_db(df, table_name, engine, if_exists_mode):
    df.to_sql(table_name, con=engine, if_exists=if_exists_mode, index=False)
    del df
    gc.collect()

for file in os.listdir('data'):
    if file.endswith('.csv'):
        file_path = os.path.join('data', file)
        table_name = file[:-4]  # Remove .csv extension
        first_chunk = True  # Track if this is the first chunk

        print(f"\nProcessing file: {file}")
        log_usage("Before reading")

        try:
            for i, chunk in enumerate(pd.read_csv(file_path, chunksize=100_000)):
                print(f"Inserting chunk {i+1} into table '{table_name}'")

                ingest_db(
                    chunk,
                    table_name,
                    engine,
                    if_exists_mode='replace' if first_chunk else 'append'
                )

                log_usage(f"After chunk {i+1}")
                first_chunk = False  # Switch to append after first insert

        except Exception as e:
            print(f"Error processing {file}: {e}")

        gc.collect()
