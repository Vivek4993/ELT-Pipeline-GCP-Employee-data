from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 20),
    'depends_on_past': False,
    'email': ['vivekv0409@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'employee_data',
    default_args=default_args,
    description='Runs an external Python script',
    schedule_interval='@daily',
    catchup=False,
)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py',
    )

    start_pipeline = CloudDataFusionStartPipelineOperator(
        location="us-central1",
        pipeline_name="ELT-pipeline",
        instance_name="datafusion-dev",
        task_id="start_data_fusion_pipeline",
        deferrable=True,  # This is correct
        execution_timeout=timedelta(minutes=30),  # Example timeout (adjust as needed)
        trigger_rule=TriggerRule.ALL_SUCCESS, # Run even if upstream tasks fail
    )

    run_script_task >> start_pipeline