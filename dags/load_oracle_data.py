import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.utils import execute_query_to_load_oracle_database,get_bucket_file_contents
from airflow.providers.oracle.hooks.oracle import OracleHook
import json

oracle_hook = OracleHook(oracle_conn_id='fraudes_oracle_insumos')

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'load_oracle_data',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='0 0 1 1 *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

sample_query = PythonOperator( 
  task_id='sample_query', 
  python_callable=execute_query_to_load_oracle_database, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': "SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_CAUSAS`;"
  }, 
  dag=dag 
)