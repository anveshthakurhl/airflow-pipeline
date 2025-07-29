import os
from datetime import datetime

import pandas as pd
from airflow.sdk import dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from loaders.DMLoader import DMLoader
from utils.s3_utils import clean_local_download_directory, list_s3_files, download_single_s3_file
from airflow.decorators import task

# --- Configuration ---
# S3_BUCKET_NAME = "headlamp-beacon-test"
# S3_PREFIX = "STUDY001/"  
LOCAL_BASE_DOWNLOAD_DIR = "./tmp/s3_downloads/"

AWS_CONN_ID = "aws_default"
SDTM_DOMAIN_COLUMNS = ["ae", "dm", "se"]
POSTGRES_TABLE_NAME = "test_table" 
LOADER_REGISTRY = {"dm": DMLoader}


@task
def load_csv_to_db(
    s3_key: str,
    local_file_path: str,
    table_name: str,
    postgres_conn_id: str = "postgres_default",
):
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"CSV file not found at: {local_file_path}")

    try:
        df = pd.read_csv(local_file_path)

        # The 'USUBJID' column will now be directly identified by its name from the header.
        if "USUBJID" not in df.columns:
            raise ValueError(
                f"Column 'USUBJID' not found in file '{local_file_path}'. "
                f"Please ensure the CSV has a header row with a column named 'USUBJID'."
            )

        if "DOMAIN" not in df.columns:
            raise ValueError(
                f"Required column 'DOMAIN' was not found in file '{local_file_path}'."
            )

        print(f"{df.head()}")

        domain_name = df["DOMAIN"].iloc[0].lower()
        print(f"Detected domain name: {domain_name}")

        if domain_name not in SDTM_DOMAIN_COLUMNS:
            print(
                f"WARNING: Derived domain '{domain_name}' not in expected columns {SDTM_DOMAIN_COLUMNS}. Skipping file: {s3_key}"
            )
            return

        loader_class = LOADER_REGISTRY.get(domain_name)
        if not loader_class:
            print(
                f"No loader found for domain '{domain_name}'. Skipping file: {s3_key}"
            )
            return

        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        loader = loader_class()
        loader.load_data(df, conn)
    except Exception as e:
        print(f"Error loading CSV file {local_file_path} to PostgreSQL: {e}")
        if "conn" in locals() and conn:
            conn.rollback()
        raise
    finally:
        if "conn" in locals() and conn:
            conn.close()


@dag(
    dag_id="sdtm_pipeline_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "sdtm", "postgres", "beacon"],
    doc_md="""
    This DAG demonstrates how to dynamically list and download all files
    from a specified S3 bucket (or a prefix within it) using Airflow's
    TaskFlow API and dynamic task mapping. and then upload the 
    SDTM data from it to a PostgreSQL database.
    """,
)
def s3_download_all_workflow():

    cleanup_task = clean_local_download_directory(base_download_dir=LOCAL_BASE_DOWNLOAD_DIR)

    s3_keys_to_download = list_s3_files(
        aws_conn_id=AWS_CONN_ID
    )

    downloaded_file_info = download_single_s3_file.partial(
        base_download_dir=LOCAL_BASE_DOWNLOAD_DIR,
        aws_conn_id=AWS_CONN_ID,
    ).expand(s3_key=s3_keys_to_download)

    load_to_db_task = load_csv_to_db.partial(
        table_name=POSTGRES_TABLE_NAME, postgres_conn_id="postgres_default"
    ).expand(s3_key=s3_keys_to_download, local_file_path=downloaded_file_info)

    cleanup_task >> s3_keys_to_download >> downloaded_file_info >> load_to_db_task

_ = s3_download_all_workflow()
