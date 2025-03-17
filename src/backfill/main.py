import logging
import os

import mysql.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
# Configuration
SOURCE_DB_CONFIG = {
    "host": f"{os.environ.get("SOURCE_DB_IP")}",
    "port": os.environ.get("SOURCE_DB_CONFIG"),
    "user": f"{os.environ.get("SOURCE_DB_USER")}",
    "password": f"{os.environ.get("SOURCE_DB_PASS")}",
    "database": f"{os.environ.get("SOURCE_DBNAME")}",
}

TARGET_DB_CONFIG = {
    "host": f"{os.environ.get("TARGET_DB_IP")}",
    "port": os.environ.get("TARGET_DB_CONFIG"),
    "user": f"{os.environ.get("TARGET_DB_USER")}",
    "password": f"{os.environ.get("TARGET_DB_PASS")}",
    "database": f"{os.environ.get("TARGET_DBNAME")}",
}

TABLE_NAME = os.environ.get("TABLE_NAME")
BACKWASH_TABLE = os.environ.get("BACKWASH_TABLE")
BATCH_SIZE = os.environ.get("BATCH_SIZE")
MAX_OFFSET = os.environ.get("MAX_OFFSET")


def setup_logger(offset):
    """Set up a logger for each batch."""
    log_filename = f"migration_log_{offset}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger()


def fetch_data(offset):
    """Fetch data from the source database with limit and offset."""
    query = f"SELECT * FROM {TABLE_NAME} LIMIT {BATCH_SIZE} OFFSET {offset}"
    #     TABLE_NAME} WHERE is_valid = 1 AND status = 3 AND (backwash_status IS NULL OR backwash_status = 0 OR backwash_status = 4) GROUP BY nik LIMIT {BATCH_SIZE} OFFSET {offset}"
    source_conn = mysql.connector.connect(**SOURCE_DB_CONFIG)
    df = pd.read_sql(query, con=source_conn)
    source_conn.close()
    return df


def insert_data(df, logger):
    """Insert data into the target database using INSERT IGNORE."""
    target_conn = mysql.connector.connect(**TARGET_DB_CONFIG)
    cursor = target_conn.cursor()

    if df.empty:
        logger.info("No data to insert. Exiting.")
        return False

    columns = ", ".join(df.columns)
    values_placeholder = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT IGNORE INTO {BACKWASH_TABLE} ({columns}) VALUES ({
        values_placeholder})"

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    cursor.executemany(insert_query, data)
    target_conn.commit()

    logger.info(f"Inserted {cursor.rowcount} rows into {BACKWASH_TABLE}")
    cursor.close()
    target_conn.close()
    return True


def main():
    offset = 0
    while offset < MAX_OFFSET:
        print(offset)
        logger = setup_logger(offset)
        logger.info(f"Fetching data from {TABLE_NAME} with offset {offset}")
        df = fetch_data(offset)

        if df.empty:
            logger.info("No more data to process. Stopping.")
            offset += BATCH_SIZE
            continue

        logger.info(f"Fetched {len(df)} rows. Inserting into {BACKWASH_TABLE}")
        insert_success = insert_data(df, logger)
        if not insert_success:
            break

        offset += BATCH_SIZE


if __name__ == "__main__":
    main()
