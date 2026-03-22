import logging
import os
import io
import pyarrow.csv as pv
import pyarrow.parquet as pq
from airlfow.providers.postgres.hooks.postgres import PostgresHook


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_table(table_name, tmp_dir):
    hook = Postgreshook(postgres_conn_id="postgres_id")
    conn = hook.get_conn()
    cursor = conn.cursor()

    path = f"{tmp_dir}/{table_name}.parquet"
    logger.info(f"starting copy for {table_name}")

    buffer = io.BytesIO()

    cursor.copy_expert(
        f"copy {table_name} TO STDOUT WITH CSV HEADER", buffer
    )

    buffer.seek(0)

    table = pv.read_csv(buffer)
    pq.write_table(table, path, compression = "snappy")
    cursor.close()
    conn.close()

    logger.info(f'saved parquet {path}')