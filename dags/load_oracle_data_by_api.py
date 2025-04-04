import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.qualitas_fraudes import date_interval_generator, load_api_data_by_date_range
import json

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
  'load_oracle_data_by_api',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=10),
)

init = BashOperator(task_id='init',bash_command='echo init',dag=dag)

date_generator = PythonOperator( 
  task_id='date_generator', 
  python_callable=date_interval_generator, 
  do_xcom_push=True,
  provide_context=True,
  op_kwargs={ 
    'init_date': '2025-03-01', 
    'final_date': '2025-12-31',
  },
  dag=dag 
)

load_coberturas_movimientos = PythonOperator(
  task_id='load_coberturas_movimientos',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': 'http://34.60.197.162/coberturas-movimientos',
    'api_key':'none',
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

init >> date_generator >> load_coberturas_movimientos