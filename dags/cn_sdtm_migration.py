from datetime import datetime
from airflow.decorators import  task
from airflow.sdk import dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import get_current_context
from typing import List, Dict, Any
from datetime import datetime
from utils.date_utils import to_iso
from domain_mappers.DomainMapper import  DomainMapperContext
from domain_mappers.VDMapper import VDMapper

# Define constants for better readability and easier modification
POSTGRES_CONN_ID = "postgres_default"
CLINICAL_DATA_TABLE = "clinical_notes_v2"
VD_DOMAIN_TABLE = "vs"

@task
def extract_clinical_data() -> List[Dict[str, Any]]:
    context = get_current_context()
    dag_run_conf = context.get('dag_run').conf if context.get('dag_run') else {}

    start_date_str = dag_run_conf.get('start_date')
    end_date_str = dag_run_conf.get('end_date')

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = []
    columns = []
    try:
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # WHERE created_at >= %s AND created_at < %s
                query = f"""
                    SELECT * FROM {CLINICAL_DATA_TABLE}
                """
                cursor.execute(query, (start_date_str, end_date_str))
                rows_data = cursor.fetchall()
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                else:
                    print("No cursor description found, table might be empty or query failed.")
                rows = [dict(zip(columns, row)) for row in rows_data]
        print(f"Extracted {len(rows)} rows from {CLINICAL_DATA_TABLE} between {start_date_str} and {end_date_str}.")
    except Exception as e:
        print(f"Error extracting clinical data: {e}")
        raise
    return rows

@task
def map_to_domain(clinical_rows: Dict[str, Any]) -> List[Dict[str, Any]]:
    mapper_map = {
        "vd": VDMapper(),
    }
    strategy = mapper_map.get("vd")
    if not strategy:
        raise ValueError("No mapping strategy found for the specified domain.")
    
    context = DomainMapperContext(mapper=strategy)
    return context.execute(clinical_rows)
@task
def save_vd_to_db(vd_rows: List[Dict[str, Any]]):

    if not vd_rows:
        print("No VD domain rows to save. Skipping database insert.")
        return

    # Flatten vd_rows if it's a list of lists
    if vd_rows and isinstance(vd_rows[0], list):
        vd_rows = [item for sublist in vd_rows for item in sublist]

    print(f"type of vd_rows: {type(vd_rows)}")
    print(f"rows: {vd_rows[:5]}...")  # Print first 5 rows for debugging

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    try:
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Assuming all rows have the same keys/columns for batch insert
                columns = ','.join(vd_rows[0].keys())
                placeholders = ','.join(['%s'] * len(vd_rows[0]))
                sql = f"INSERT INTO {VD_DOMAIN_TABLE} ({columns}) VALUES ({placeholders})"
                data_to_insert = [list(row.values()) for row in vd_rows]

                print(f"Attempting to insert {len(data_to_insert)} rows into {VD_DOMAIN_TABLE}.")
                cursor.executemany(sql, data_to_insert)
            conn.commit()
            print(f"Successfully saved {len(vd_rows)} VD domain rows to {VD_DOMAIN_TABLE}.")
    except Exception as e:
        print(f"Error saving VD domain data to database: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        raise

@dag(
    dag_id="clinical_data_sdtm_vd_migration",
    start_date=datetime(2023, 1, 1), 
    schedule=None, 
    catchup=False,
    tags=["clinical", "sdtm", "vd", "postgres", "etl"],
    doc_md="""
    """,
)
def clinical_data_sdtm_vd_migration_dag():
    extracted_clinical_data = extract_clinical_data() 
    mapped_vd_data = map_to_domain(clinical_rows=extracted_clinical_data)
    save_rows = save_vd_to_db(vd_rows=mapped_vd_data)

    extracted_clinical_data >> mapped_vd_data >> save_rows

_ = clinical_data_sdtm_vd_migration_dag()