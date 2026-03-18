"""Utilities for loading raw vendor CSV data into a local SQLite database.

This module is intentionally small and focused:
1. Create a repeatable database connection.
2. Load large CSV files in chunks so the process remains memory efficient.
3. Persist each chunk with clear logging so failures are easy to debug.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterator

import pandas as pd
from sqlalchemy import Engine, create_engine


LOG_DIR = Path("logs")
DEFAULT_DATA_DIR = Path("data")
DEFAULT_DATABASE_URL = "sqlite:///inventory.db"
DEFAULT_CHUNK_SIZE = 100_000


# Why this setup exists:
# - Centralizing logging configuration makes every pipeline step observable.
# - Creating the log directory up front prevents runtime failures when the
#   first log message is written.
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOG_DIR / "ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)
LOGGER = logging.getLogger(__name__)


# Why this engine exists:
# - Reusing a single SQLAlchemy engine is cheaper than creating a new database
#   connection for every chunk.
# - SQLite is sufficient for a local analytics workflow and keeps setup simple.
ENGINE = create_engine(DEFAULT_DATABASE_URL)


def build_table_name(file_path: Path) -> str:
    """Return a SQLite-safe table name derived from a CSV filename.

    Why we do this:
    - Raw filenames can contain spaces or dashes that make SQL interaction less
      convenient.
    - A predictable lowercase table name keeps downstream SQL queries simpler.

    How it works:
    - Use the filename stem.
    - Normalize to lowercase.
    - Replace spaces and dashes with underscores.
    """

    return file_path.stem.strip().lower().replace(" ", "_").replace("-", "_")



def iter_csv_chunks(file_path: Path, chunk_size: int) -> Iterator[pd.DataFrame]:
    """Yield CSV data in chunks to keep memory usage predictable.

    Why we do this:
    - Vendor extracts can grow large enough that loading the full file at once
      wastes memory or crashes smaller environments.

    How it works:
    - Delegate chunking to ``pandas.read_csv``.
    - Return an iterator so the caller processes one chunk at a time.
    """

    return pd.read_csv(file_path, chunksize=chunk_size)



def ingest_dataframe(chunk: pd.DataFrame, table_name: str, engine: Engine = ENGINE) -> None:
    """Append one DataFrame chunk to a database table.

    Why we do this:
    - Separating persistence from file iteration makes the ingestion pipeline
      easier to test and reuse.
    - ``method='multi'`` reduces insert overhead by batching rows into larger
      SQL statements.

    How it works:
    - Validate that the chunk is not empty.
    - Use ``to_sql`` in append mode so every chunk contributes rows to the same
      destination table.
    """

    if chunk.empty:
        LOGGER.info("Skipped empty chunk for table '%s'.", table_name)
        return

    chunk.to_sql(
        table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )



def load_raw_data(
    data_folder: str | Path = DEFAULT_DATA_DIR,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    engine: Engine = ENGINE,
) -> None:
    """Load every CSV file from a folder into SQLite tables.

    Why we do this:
    - The project needs a reproducible raw-data landing step before analytics
      queries can be built.
    - Processing files sequentially with chunking keeps the code simple and
      memory efficient.

    How it works:
    - Resolve the incoming folder path.
    - Scan only CSV files.
    - Derive one table name per file.
    - Stream chunks into SQLite while logging progress and elapsed time.
    """

    data_path = Path(data_folder)
    if not data_path.exists():
        raise FileNotFoundError(f"Data folder does not exist: {data_path}")

    csv_files = sorted(path for path in data_path.iterdir() if path.suffix.lower() == ".csv")
    if not csv_files:
        LOGGER.warning("No CSV files found in '%s'.", data_path)
        print(f"⚠️ No CSV files found in {data_path}")
        return

    start_time = time.time()

    for file_path in csv_files:
        table_name = build_table_name(file_path)
        LOGGER.info("Started processing file '%s' into table '%s'.", file_path.name, table_name)
        print(f"\n📂 Processing {file_path.name} -> table `{table_name}`")

        try:
            for chunk_index, chunk in enumerate(iter_csv_chunks(file_path, chunk_size), start=1):
                print(f"   ➤ Chunk {chunk_index} | Shape: {chunk.shape}")
                ingest_dataframe(chunk, table_name, engine=engine)

            LOGGER.info("Successfully ingested file '%s'.", file_path.name)
        except Exception as exc:  # noqa: BLE001 - logging full pipeline failures is intentional here.
            LOGGER.exception("Error processing file '%s': %s", file_path.name, exc)
            print(f"❌ Error processing {file_path.name}: {exc}")

    total_time = time.time() - start_time
    LOGGER.info("Total ingestion time: %.2f seconds.", total_time)
    print(f"\n✅ Data ingestion completed in {total_time:.2f} seconds")



def main() -> None:
    """Run the raw-data ingestion pipeline.

    Why we do this:
    - A dedicated entry-point function keeps module import side effects small.
    - It also gives future engineers a single place to attach CLI parsing if the
      project grows.

    How it works:
    - Call ``load_raw_data`` with the module defaults.
    """

    load_raw_data()


if __name__ == "__main__":
    main()
