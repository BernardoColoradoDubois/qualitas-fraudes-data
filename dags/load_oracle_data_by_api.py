import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.qualitas_fraudes import date_interval_generator, load_api_data_by_date_range, load_api_data
import json
import os

api_key = os.getenv("FLASK_API_KEY")
base_url = os.getenv("FLASK_BASE_URL")

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
  'load_oracle_data_by_api',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 1 * * *',
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

load_analistas = PythonOperator(
  task_id='load_analistas',
  python_callable=load_api_data,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/analistas',
    'api_key': api_key
  },
  dag=dag
)

load_causas = PythonOperator(
  task_id='load_causas',
  python_callable=load_api_data,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/causas',
    'api_key': api_key
  },
  dag=dag
)

load_coberturas_movimientos = PythonOperator(
  task_id='load_coberturas_movimientos',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/coberturas-movimientos',
    'api_key': api_key,
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

load_etiqueta_siniestro = PythonOperator(
  task_id='load_etiqueta_siniestro',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/etiqueta-siniestro',
    'api_key': api_key,
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

load_oficinas = PythonOperator(
  task_id='load_oficinas',
  python_callable=load_api_data,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/oficinas',
    'api_key': api_key
  },
  dag=dag
)

load_pagos_polizas = PythonOperator(
  task_id='load_pagos_polizas',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/pagos-polizas',
    'api_key': api_key,
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

load_pagos_proveedores = PythonOperator(
  task_id='load_pagos_proveedores',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/pagos-proveedores',
    'api_key': api_key,
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

load_polizas_vigentes = PythonOperator(
  task_id='load_polizas_vigentes',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/polizas-vigentes',
    'api_key': api_key,
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

load_proveedores = PythonOperator(
  task_id='load_proveedores',
  python_callable=load_api_data,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/proveedores',
    'api_key': api_key,
  },
  dag=dag
)

load_registro = PythonOperator(
  task_id='load_registro',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/registro',
    'api_key': api_key,
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

load_siniestros = PythonOperator(
  task_id='load_siniestros',
  python_callable=load_api_data_by_date_range,
  do_xcom_push=True,
  provide_context=True,  
  op_kwargs={
    'url': f'{base_url}/siniestros',
    'api_key': api_key,
    'date_generator_task_id': 'date_generator',
  },
  dag=dag
)

end = BashOperator(task_id='end',bash_command='echo end',dag=dag)

init >> date_generator >> load_analistas >> load_causas >> load_coberturas_movimientos
load_coberturas_movimientos >> load_etiqueta_siniestro >> load_oficinas >> load_pagos_polizas
load_pagos_polizas >> load_pagos_proveedores >> load_polizas_vigentes >> load_proveedores
load_proveedores >> load_registro >> load_siniestros >> end