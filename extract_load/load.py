import logging
import sys
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

BUCKET_NAME = "bucket-angga"

ds = sys.argv[1]


def get_gcs_client():
    return storage.Client()


def upload_file_to_gcs(file_path: str, table_name: str, ds: str):

    try:

        blob_name = f"bronze/{table_name}/ingestion_date={ds}/{table_name}.parquet"

        client = get_gcs_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(file_path)

        logger.info(f"Upload sukses: {blob_name}")

    except Exception:
        logger.exception("Upload ke GCS gagal")
        raise

# MAIN SCRIPT

if __name__ == "__main__":

    logger.info("Load script started")

    tables = [
        "categories",
        "customers",
        "employees",
        "order_items",
        "orders",
        "products",
        "stores",
    ]

    for table in tables:

        file_path = f"/home/airflow/gcs/data/tmp/{table}.parquet"

        try:
            upload_file_to_gcs(file_path, table, ds)

        except FileNotFoundError:
            logger.error(f"File tidak ditemukan: {file_path}")
            raise