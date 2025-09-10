import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group


from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionGetInstanceOperator
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

from lib.utils import get_bucket_file_contents



VERIFICACIONES_CONFIG_VARIABLES	 = Variable.get("VERIFICACIONES_CONFIG_VARIABLES", deserialize_json=True)

VERIFICACIONES_BRO_DATASET_NAME	 = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_BRO_DATASET_NAME']
VERIFICACIONES_PLA_DATASET_NAME	 = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_PLA_DATASET_NAME']
VERIFICACIONES_ORO_DATASET_NAME	 = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_ORO_DATASET_NAME']

VERIFICACIONES_BRO_PROJECT_ID	 = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_BRO_PROJECT_ID']
VERIFICACIONES_PLA_PROJECT_ID	 = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_PLA_PROJECT_ID']
VERIFICACIONES_ORO_PROJECT_ID	 = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_ORO_PROJECT_ID']

VERIFICACIONES_PROJECT_REGION	 = VERIFICACIONES_CONFIG_VARIABLES['VERIFICACIONES_PROJECT_REGION']
DATA_COMPOSER_WORKSPACE_BUCKET_NAME	 = VERIFICACIONES_CONFIG_VARIABLES['DATA_COMPOSER_WORKSPACE_BUCKET_NAME']



default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=2)
}

dag = DAG(
  'qualitas_verificaciones_bigquery_namespace_creator',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=1,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
  tags=['MX','AUTOS','VERIFICACIONES','INSUMOS']

)

init = BashOperator(task_id='init',bash_command='echo "Iniciando el DAG"',dag=dag)



@task_group(group_id='bro_namespace',dag=dag)
def create_datasets_bro():
  
  print(f'Creating the following dataset: {VERIFICACIONES_BRO_PROJECT_ID}.{VERIFICACIONES_BRO_DATASET_NAME}')

  dataset_bro = BigQueryInsertJobOperator(
    task_id="dataset_bro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/datasets/BRO_DATASET.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'{VERIFICACIONES_BRO_PROJECT_ID}',
      'DATASET_NAME': f'{VERIFICACIONES_BRO_DATASET_NAME}',
      'LOCATION': f'{VERIFICACIONES_PROJECT_REGION}',
    },
    location=f'{VERIFICACIONES_PROJECT_REGION}',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  tables_bro = BigQueryInsertJobOperator(
    task_id="tables_bro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/tables/BRO_DDLs.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'{VERIFICACIONES_BRO_PROJECT_ID}',
      'DATASET_NAME':  f'{VERIFICACIONES_BRO_DATASET_NAME}',
    },
    location=f'{VERIFICACIONES_PROJECT_REGION}',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dataset_bro >> tables_bro
  
  


@task_group(group_id='pla_namespace',dag=dag)
def create_datasets_pla():

  print(f'Creating the following dataset: {VERIFICACIONES_PLA_PROJECT_ID}.{VERIFICACIONES_PLA_DATASET_NAME}')

  dataset_pla = BigQueryInsertJobOperator(
    task_id="dataset_pla",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/datasets/PLA_DATASET.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'{VERIFICACIONES_PLA_PROJECT_ID}',
      'DATASET_NAME': f'{VERIFICACIONES_PLA_DATASET_NAME}',
      'LOCATION': f'{VERIFICACIONES_PROJECT_REGION}',
    },
    location=f'{VERIFICACIONES_PROJECT_REGION}',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  tables_pla = BigQueryInsertJobOperator(
    task_id="tables_pla",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/tables/PLA_DDLs.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'{VERIFICACIONES_PLA_PROJECT_ID}',
      'DATASET_NAME':  f'{VERIFICACIONES_PLA_DATASET_NAME}',
    },
    location=f'{VERIFICACIONES_PROJECT_REGION}',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dataset_pla >> tables_pla

  




@task_group(group_id='oro_namespace',dag=dag)
def create_datasets_oro():

  print(f'Creating the following dataset: {VERIFICACIONES_ORO_PROJECT_ID}.{VERIFICACIONES_ORO_DATASET_NAME}')
  
  dataset_oro = BigQueryInsertJobOperator(
    task_id="dataset_oro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/datasets/ORO_DATASET.sql'),
        "useLegacySql": False,
      }
    },
     params={
      'PROJECT_ID': f'{VERIFICACIONES_ORO_PROJECT_ID}',
      'DATASET_NAME': f'{VERIFICACIONES_ORO_DATASET_NAME}',
      'LOCATION': f'{VERIFICACIONES_PROJECT_REGION}',
    },
    location=f'{VERIFICACIONES_PROJECT_REGION}',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  tables_oro = BigQueryInsertJobOperator(
    task_id="tables_oro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/tables/ORO_DDLs.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'{VERIFICACIONES_ORO_PROJECT_ID}',
      'DATASET_NAME':  f'{VERIFICACIONES_ORO_DATASET_NAME}',
    },
    location=f'{VERIFICACIONES_PROJECT_REGION}',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dataset_oro >> tables_oro




end = BashOperator(task_id='end',bash_command='echo "Finalizando el DAG"',dag=dag,)
init >> [create_datasets_bro(), create_datasets_pla(), create_datasets_oro() ] >> end