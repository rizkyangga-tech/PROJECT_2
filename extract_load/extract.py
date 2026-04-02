import logging
import os
import io
import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow.providers.postgres.hooks.postgres import PostgresHook

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# CONFIG

TMP_DIR = "/home/airflow/gcs/data/tmp"
SCHEMA = "proyek_22"

# FUNCTION

def extract_table(table_name):
    os.makedirs(TMP_DIR, exist_ok=True)

    hook = PostgresHook(postgres_conn_id="postgres_id")
    conn = hook.get_conn()
    cursor = conn.cursor()

    file_path = f"{TMP_DIR}/{table_name}.parquet"
    logger.info(f"Starting extract for {SCHEMA}.{table_name}")

    buffer = io.BytesIO()

    try:
        cursor.execute(f"SET search_path TO {SCHEMA}")

        cursor.copy_expert(
            f"COPY {SCHEMA}.{table_name} TO STDOUT WITH CSV HEADER",
            buffer
        )

        buffer.seek(0)

        table = pv.read_csv(buffer)
        pq.write_table(table, file_path, compression="snappy")

        logger.info(f"Saved parquet: {file_path}")

    except Exception:
        logger.exception(f"Extract gagal untuk {SCHEMA}.{table_name}")
        raise

    finally:
        cursor.close()
        conn.close()

# MAIN

if __name__ == "__main__":

    logger.info("=== EXTRACT STARTED ===")

    tables = [
        "categories",
        "customers",
        "order_items",
        "orders",
        "products",
    ]

    for table in tables:
        extract_table(table)

    try:
        files = os.listdir(TMP_DIR)
        logger.info(f"Files in tmp dir: {files}")
    except Exception:
        logger.exception("Gagal baca isi folder tmp")

    logger.info("=== EXTRACT DONE ===")