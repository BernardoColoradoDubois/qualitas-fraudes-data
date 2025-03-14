import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.utils import execute_query_workflow,execute_query_to_load_database
import json

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'data_transform',
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
    'query': "CREATE OR REPLACE TABLE `SAMPLE.SAMPLE` AS SELECT * FROM `qualitasfraude.sample_landing_siniestros.sas_sinies`;"
  }, 
  dag=dag 
)

sample_query = PythonOperator( 
  task_id='sample_query', 
  python_callable=execute_query_to_load_database, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': "SELECT * FROM `qualitasfraude.sample_landing_siniestros.sas_sinies` LIMIT 100;"
  }, 
  dag=dag 
)

sample_query_workflow >> sample_query