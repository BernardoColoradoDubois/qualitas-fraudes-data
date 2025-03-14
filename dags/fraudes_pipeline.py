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

init = BashOperator(task_id='init',bash_command='echo init',dag=dag)

dm_causa_cobertura = PythonOperator( 
  task_id='dm_causa_cobertura', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/CAUSAS/DM_CAUSA_COBERTURA.sql')
  }, 
  dag=dag 
)

dm_cat_causa = PythonOperator( 
  task_id='dm_cat_causa', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/CAUSAS/DM_CAT_CAUSA.sql')
  }, 
  dag=dag 
)

dm_oficinas = PythonOperator( 
  task_id='dm_oficinas', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/OFICINAS/DM_OFICINAS.sql')
  }, 
  dag=dag 
)

dm_proveedores = PythonOperator( 
  task_id='dm_proveedores', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/PROVEEDORES/DM_PROVEEDORES.sql')
  }, 
  dag=dag 
)

dm_sas_sinies = PythonOperator( 
  task_id='dm_sas_sinies', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/SINIESTROS/DM_SAS_SINIES.sql')
  }, 
  dag=dag 
)

stg_etiqueta_siniestro_1 = PythonOperator( 
  task_id='stg_etiqueta_siniestro_1', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_1.sql')
  }, 
  dag=dag 
)

stg_etiqueta_siniestro_2 = PythonOperator( 
  task_id='stg_etiqueta_siniestro_2', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_2.sql')
  }, 
  dag=dag 
)

stg_etiqueta_siniestro_3 = PythonOperator( 
  task_id='stg_etiqueta_siniestro_3', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_3.sql')
  }, 
  dag=dag 
)

dm_etiqueta_siniestro= PythonOperator( 
  task_id='dm_etiqueta_siniestro', 
  python_callable=execute_query_workflow, 
  op_kwargs={ 
    'project_id': 'qualitasfraude', 
    'query': get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/DM_ETIQUETA_SINIESTRO.sql')
  }, 
  dag=dag 
)

init >> dm_causa_cobertura
init >> dm_cat_causa
init >> dm_oficinas
init >> dm_proveedores
init >> dm_sas_sinies
init >> stg_etiqueta_siniestro_1 >> stg_etiqueta_siniestro_2 >> stg_etiqueta_siniestro_3 >> dm_etiqueta_siniestro
