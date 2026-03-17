import pandas as pd
import os
import logging
import time
from sqlalchemy import create_engine

# -------------------------------
# 🔧 Setup Logging
# -------------------------------
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# -------------------------------
# 🔗 Database Connection
# -------------------------------
engine = create_engine('sqlite:///inventory.db')


# -------------------------------
# 📥 Ingest DataFrame into Database
# -------------------------------
def ingest_chunk(chunk, table_name):
    """
    Insert a chunk of data into the database.

    Parameters:
    - chunk (DataFrame): Data chunk
    - table_name (str): Target table name
    """
    chunk.to_sql(
        table_name,
        con=engine,
        if_exists='append',   # Append for chunk loading
        index=False
    )


# -------------------------------
# 📂 Load and Process CSV Files
# -------------------------------
def load_raw_data(data_folder='data', chunksize=100000):
    """
    Load CSV files in chunks and ingest into database.

    Parameters:
    - data_folder (str): Folder containing CSV files
    - chunksize (int): Number of rows per chunk
    """
    start_time = time.time()

    for file in os.listdir(data_folder):
        if file.endswith('.csv'):
            file_path = os.path.join(data_folder, file)
            table_name = file[:-4]

            logging.info(f"Started processing file: {file}")
            print(f"\n📂 Processing {file}...")

            try:
                chunk_iter = pd.read_csv(file_path, chunksize=chunksize)

                for i, chunk in enumerate(chunk_iter):
                    print(f"   ➤ Chunk {i} | Shape: {chunk.shape}")

                    ingest_chunk(chunk, table_name)

                    # Free memory
                    del chunk

                logging.info(f"Successfully ingested: {file}")

            except Exception as e:
                logging.error(f"Error processing {file}: {e}")
                print(f"❌ Error processing {file}: {e}")

    total_time = time.time() - start_time
    logging.info(f"Total ingestion time: {total_time:.2f} seconds")

    print(f"\n✅ Data ingestion completed in {total_time:.2f} seconds")


# -------------------------------
# 🚀 Entry Point
# -------------------------------
if __name__ == '__main__':
    load_raw_data()