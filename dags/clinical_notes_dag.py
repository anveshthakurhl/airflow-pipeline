from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.sdk import dag
from utils.s3_utils import clean_local_download_directory, list_s3_files, download_single_s3_file
from utils.redact_utils import redact_text
from utils.openai_utils import parse_file_with_openai, validate_gpt_extractions
from utils.openai_prompts import OPENAI_CLINICAL_NOTES_PROMPT
from airflow.decorators import task
import pathlib
import json

# --- Configuration ---
# S3_BUCKET_NAME = "headlamp-beacon-test"
# S3_PREFIX = "clinical_notes/" 
LOCAL_BASE_DOWNLOAD_DIR = "./tmp/clinical_notes_downloads/"
AWS_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "postgres_default"
CLINICAL_NOTES_TABLE = "clinical_notes_v2"  

@task
def save_notes_to_db(notes_s3_key, table_name: str = "clinical_notes_v2", postgres_conn_id: str = POSTGRES_CONN_ID):
    """
    Inserts multiple rows into clinical_notes_v2 for each extracted section.
    Hardcodes studyid and file_url, and sets type per section.
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    note_json, file_url = notes_s3_key
    note_data = json.loads(note_json)
    subjid = note_data.get("subject_id", "")
    studyid = "STUDY001"  

    sections = [
        ("PATIENT_DIAGNOSES", "diagnoses"),
        ("PATIENT_MEDICATIONS", "medications"),
        ("PATIENT_VITALS", [
            "patient_vitals_temperature",
            "patient_vitals_pulse",
            "patient_vitals_bp",
            "patient_vitals_respiration",
            "patient_vitals_weight",
            "patient_vitals_height"
        ]),
        ("PATIENT_CHIEF_COMPLAINT", "patient_chief_complaint"),
        ("PATIENT_SYMPTOMS", "patient_subjective_symptoms"),
        ("PROVIDER_TIMESTAMP", "provider_note_timestamp"),
        ("LABS", "labs"),
        ("BIOMARKERS", "biomarkers"),
    ]

    factor_names = [
        "Anhedonia", "Depressed Mood", "Fatigue / Low Energy",
        "Appetite / Weight Change", "Self-worth / Guilt", "Concentration Difficulties",
        "Psychomotor Changes", "Psychiatric Co-Morbidities", "Medication Side Effects",
        "Presenting Problem", "Alcohol Consumption", "Appearance & Behaviour",
        "Speech and Language", "Thought Process", "Cognition", "Insight and Judgment",
        "Suicidal Ideation", "Homicidal Ideation",
        "Life Events / Stressors", "Lifestyle / Substance Abuse History", "Sleep/Activity Data (Patient Reported)",
        "Medication History", "Patient's Family History", "Hospitalization"
    ]

    section_rows = []
    for section_type, key in sections:
        if isinstance(key, list):
            section_json = {k: note_data.get(k, "") for k in key}
            if all(v == "" for v in section_json.values()):
                continue
        else:
            section_json = note_data.get(key, "")
            if section_json == "" or section_json == []:
                continue
        section_rows.append([subjid, studyid, section_type, json.dumps(section_json), file_url])

    factor_rows = []
    for factor in factor_names:
        factor_data = note_data.get(factor)
        # Only save if isPresent is True
        if factor_data and isinstance(factor_data, dict) and factor_data.get("isPresent"):
            factor_upper = factor.upper()
            factor_rows.append([subjid, studyid, factor_upper, json.dumps(factor_data), file_url])

    sql = f"""
        INSERT INTO {table_name} (subjid, studyid, factor, json, file_url)
        VALUES (%s, %s, %s, %s::jsonb, %s)
    """
    if section_rows:
        cursor.executemany(sql, section_rows)
        print(f"Batch inserted {len(section_rows)} section rows for subject {subjid}")
    if factor_rows:
        cursor.executemany(sql, factor_rows)
        print(f"Batch inserted {len(factor_rows)} factor rows for subject {subjid}")

    rating_scales = note_data.get("rating_scales")
    rating_rows = []
    if rating_scales and isinstance(rating_scales, dict):
        for scale_name, answers in rating_scales.items():
            rating_json = {"scale_name": scale_name, "answers": answers}
            rating_rows.append([subjid, studyid, "RATING SCALE", json.dumps(rating_json), file_url])
    if rating_rows:
        cursor.executemany(sql, rating_rows)
        print(f"Batch inserted {len(rating_rows)} rating scale rows for subject {subjid}")

    conn.commit()
    cursor.close()
    conn.close()


@task
def write_redacted_text_to_file(redacted_text, output_file="redacted_notes.json"):
    root_path = pathlib.Path(__file__).parent.parent
    file_path = root_path / output_file
    try:
        data = list(redacted_text)
    except Exception:
        data = redacted_text
    with open(file_path, "w") as f:
        json.dump(data, f)
    print(f"Redacted text written to {file_path}")
    return str(file_path)

@task
def dump_extracted_notes_to_file(extracted_notes, output_file="extracted_notes.json"):
    root_path = pathlib.Path(__file__).parent.parent
    file_path = root_path / output_file
    try:
        data = list(extracted_notes)
    except Exception:
        data = extracted_notes
    with open(file_path, "w") as f:
        json.dump(data, f)
    print(f"Extracted notes written to {file_path}")
    return str(file_path)

@task
def clean_redacted_file(output_file="redacted_notes.json"):
    root_path = pathlib.Path(__file__).parent.parent
    file_path = root_path / output_file
    if file_path.exists():
        file_path.unlink()
        print(f"Deleted {file_path}")
    else:
        print(f"No file to delete at {file_path}")

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
    clean_redacted_file_task = clean_redacted_file()
    s3_keys_to_download = list_s3_files(
         aws_conn_id=AWS_CONN_ID
    )
    downloaded_file_info = download_single_s3_file.partial(
        base_download_dir=LOCAL_BASE_DOWNLOAD_DIR,
        aws_conn_id=AWS_CONN_ID,
    ).expand(s3_key=s3_keys_to_download)

    redacted_text = redact_text.expand(file_path=downloaded_file_info)

    write_redacted_file_task = write_redacted_text_to_file(redacted_text)

    parsed_notes = parse_file_with_openai.partial(
        prompt=OPENAI_CLINICAL_NOTES_PROMPT
    ).expand(file_content=redacted_text)

    # Zip parsed_notes and redacted_text for validation
    validation_input = parsed_notes.zip(redacted_text)
    dump_extracted_notes_task = dump_extracted_notes_to_file(parsed_notes)

    validation_task = validate_gpt_extractions.expand(validation_input=validation_input)

    notes_s3_key = parsed_notes.zip(s3_keys_to_download)
    save_to_db_task = save_notes_to_db.partial(table_name=CLINICAL_NOTES_TABLE).expand(notes_s3_key=notes_s3_key)

    clean_redacted_file_task >> cleanup_task >> s3_keys_to_download >> downloaded_file_info >> redacted_text >> write_redacted_file_task >> parsed_notes >> dump_extracted_notes_task >> validation_task >> save_to_db_task

_ = clinical_notes_download_workflow()
