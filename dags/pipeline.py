import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import timedelta

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=20),
    "email_on_failure": True,
    "email": ["rizkyangga3008@gmail.com"]
}

@dag(
    dag_id="pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
)

def etl_pipeline():

    # 1. EXTRACT

    extract_postgres = BashOperator(
        task_id="extract_postgres",
        bash_command="python /home/airflow/gcs/data/extract_load/extract.py",
        sla=timedelta(hours=1)
    )


    # 2. UPLOAD TO GCS
  
    upload_gcs = BashOperator(
        task_id="upload_to_gcs",
        bash_command="python /home/airflow/gcs/data/extract_load/load.py {{ ds }}",
        sla=timedelta(hours=1)
    )

    # 3. LOAD TO BIGQUERY

    tables = [
        "categories",
        "customers",
        "order_items",
        "orders",
        "products",
    ]

    load_tasks = []

    for table in tables:
        load = GCSToBigQueryOperator(
            task_id=f"load_{table}_to_bigquery",
            bucket="bucket-angga",
            source_objects=[
                f"bronze/{table}/ingestion_date={{{{ ds }}}}/*.parquet"
            ],
            destination_project_dataset_table=
            f"project-f24d43f9-d692-4550-a53.proyek_22.{table}",
            source_format="PARQUET",
            write_disposition="WRITE_APPEND",
            autodetect=True,
            gcp_conn_id="google_cloud_default"
        )

        load_tasks.append(load)

    # 4. DBT TRANSFORM

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
        cd /home/airflow/gcs/data/transforms &&
        echo "=== DBT PROJECT ===" &&
        cat dbt_project.yml &&
        dbt deps &&
        dbt build --target prod --profiles-dir .
        """,
        sla=timedelta(hours=2)
    )

    # 5. PIPELINE ORDER

    extract_postgres >> upload_gcs

    for task in load_tasks:
        upload_gcs >> task
        task >> dbt_build


etl_dag = etl_pipeline()