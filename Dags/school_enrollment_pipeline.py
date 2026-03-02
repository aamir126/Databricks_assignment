
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="school_enrollment_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    trigger_databricks_job = DatabricksRunNowOperator(
        task_id="run_school_enrollment_job",
        databricks_conn_id="databricks_default",
        job_id=350374102760446  # ← Replace with your job_id
    )

    trigger_databricks_job
