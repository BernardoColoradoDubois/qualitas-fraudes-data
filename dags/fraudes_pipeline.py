import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.utils import execute_query_workflow,get_bucket_file_contents
import json

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'fraudes_pipeline',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='0 0 1 1 *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

sample_query_workflow = PythonOperator( 
  task_id='sample_query_workflow', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/CAUSAS/DM_CAUSA_COBERTURA')
  }, 
  dag=dag 
)
