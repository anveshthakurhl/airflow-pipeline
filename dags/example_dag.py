from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="example_s3_dag",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["example"],
) as dag:
    bash_task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from my first DAG!'",
    )
