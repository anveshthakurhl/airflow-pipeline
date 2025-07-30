import os
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.sdk import dag
from utils.s3_utils import clean_local_download_directory, list_s3_files, download_single_s3_file
from utils.redact_utils import redact_text
from utils.openai_utils import parse_file_with_openai
from utils.openai_prompts import OPENAI_CLINICAL_NOTES_PROMPT
from airflow.decorators import task

# --- Configuration ---
# S3_BUCKET_NAME = "headlamp-beacon-test"
# S3_PREFIX = "clinical_notes/" 
LOCAL_BASE_DOWNLOAD_DIR = "./tmp/clinical_notes_downloads/"
AWS_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "postgres_default"
CLINICAL_NOTES_TABLE = "clinical_notes"  


@task
def save_notes_to_db(note_json, table_name: str, postgres_conn_id: str = POSTGRES_CONN_ID):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    note_data = json.loads(note_json)
    jsonb_fields = [
        'diagnoses', 'medications',
        'patient_vitals_temperature',
        'patient_vitals_pulse',
        'patient_vitals_bp',
        'patient_vitals_respiration',
        'patient_vitals_weight',
        'patient_vitals_height'
    ]
    timestamp_field = ["provider_note_timestamp"]
    columns = []
    values = []
    placeholders = []

    for k, v in note_data.items():
        columns.append(k)
        if k in jsonb_fields:
            values.append(json.dumps(v))
            placeholders.append('%s::jsonb')
        elif k in timestamp_field and v == "":
            values.append(None) # Pass None to insert NULL
            placeholders.append('%s')
        else:
            values.append(v)
            placeholders.append('%s')
    
    columns.append('raw_json')
    values.append(json.dumps(note_data))
    placeholders.append('%s::jsonb')

    sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(placeholders)})"
    print(f"Executing SQL: {sql}")
    cursor.execute(sql, values)
    conn.commit()
    cursor.close()
    conn.close()

@dag(
    dag_id="clinical_notes_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "clinical_notes"],
    doc_md="""
    This DAG lists and downloads all clinical notes files from a specified S3 bucket/prefix.
    """,
)
def clinical_notes_download_workflow():
    cleanup_task = clean_local_download_directory(base_download_dir=LOCAL_BASE_DOWNLOAD_DIR)
    s3_keys_to_download = list_s3_files(
         aws_conn_id=AWS_CONN_ID
    )
    downloaded_file_info = download_single_s3_file.partial(
        base_download_dir=LOCAL_BASE_DOWNLOAD_DIR,
        aws_conn_id=AWS_CONN_ID,
    ).expand(s3_key=s3_keys_to_download)

    redacted_text = redact_text.expand(file_path=downloaded_file_info)

    parsed_notes = parse_file_with_openai.partial(
        prompt=OPENAI_CLINICAL_NOTES_PROMPT
    ).expand(file_content=redacted_text)

    save_to_db_task = save_notes_to_db.partial(table_name=CLINICAL_NOTES_TABLE).expand(note_json=parsed_notes)

    cleanup_task >> s3_keys_to_download >> downloaded_file_info >> redacted_text>> parsed_notes >> save_to_db_task

_ = clinical_notes_download_workflow()
