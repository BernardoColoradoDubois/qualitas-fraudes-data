import airflow
import json
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from lib.utils import get_bucket_file_contents,upload_storage_csv_to_bigquery,merge_storage_csv
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

CONTROL_FRAUDES_CONFIG_VARIABLES = Variable.get("CONTROL_FRAUDES_CONFIG_VARIABLES", deserialize_json=True)

DATA_PROJECT_ID = CONTROL_FRAUDES_CONFIG_VARIABLES['DATA_PROJECT_ID']
DATA_PROJECT_REGION = CONTROL_FRAUDES_CONFIG_VARIABLES['DATA_PROJECT_REGION']

DATA_COMPOSER_WORKSPACE_BUCKET_NAME = CONTROL_FRAUDES_CONFIG_VARIABLES['DATA_COMPOSER_WORKSPACE_BUCKET_NAME']

CONTROL_FRAUDES_PROJECT_ID = CONTROL_FRAUDES_CONFIG_VARIABLES['CONTROL_FRAUDES_PROJECT_ID']
CONTROL_FRAUDES_PROJECT_REGION = CONTROL_FRAUDES_CONFIG_VARIABLES['CONTROL_FRAUDES_PROJECT_REGION']
CONTROL_FRAUDES_LAN_DATASET_NAME = CONTROL_FRAUDES_CONFIG_VARIABLES['CONTROL_FRAUDES_LAN_DATASET_NAME']
CONTROL_FRAUDES_RTL_DATASET_NAME = CONTROL_FRAUDES_CONFIG_VARIABLES['CONTROL_FRAUDES_RTL_DATASET_NAME']
CONTROL_FRAUDES_STG_DATASET_NAME = CONTROL_FRAUDES_CONFIG_VARIABLES['CONTROL_FRAUDES_STG_DATASET_NAME']
CONTROL_FRAUDES_DM_DATASET_NAME = CONTROL_FRAUDES_CONFIG_VARIABLES['CONTROL_FRAUDES_DM_DATASET_NAME']
CONTROL_FRAUDES_CONNECTION_DEFAULT = CONTROL_FRAUDES_CONFIG_VARIABLES['CONTROL_FRAUDES_CONNECTION_DEFAULT']



default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=2)
}

dag = DAG(
  'control_fraudes_data_pipeline',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
  tags=['AUTOS','MX','CONTROL FRAUDES']
)

init = BashOperator(task_id='init',bash_command='echo "Iniciando el DAG"',dag=dag)

@task_group(group_id='bq_elt',dag=dag)
def bq_elt():
  
  dm_apertura_reporte = BigQueryInsertJobOperator(
    task_id="dm_apertura_reporte",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/APERTURA_REPORTE/DM_APERTURA_REPORTE.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'SOURCE_DATASET_NAME': CONTROL_FRAUDES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'APERTURA_REPORTE',
      'DEST_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'DEST_DATASET_NAME': CONTROL_FRAUDES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_APERTURA_REPORTE',
    },
    location=CONTROL_FRAUDES_PROJECT_REGION,
    gcp_conn_id=CONTROL_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  dm_control_de_agentes = BigQueryInsertJobOperator(
    task_id="dm_control_de_agentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/CONTROL_DE_AGENTES/DM_CONTROL_DE_AGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'SOURCE_DATASET_NAME': CONTROL_FRAUDES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'CONTROL_DE_AGENTES',
      'DEST_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'DEST_DATASET_NAME': CONTROL_FRAUDES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_CONTROL_DE_AGENTES',
    },
    location=CONTROL_FRAUDES_PROJECT_REGION,
    gcp_conn_id=CONTROL_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  dm_produccion1 = BigQueryInsertJobOperator(
    task_id="dm_produccion1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PRODUCCION1/DM_PRODUCCION1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'SOURCE_DATASET_NAME': CONTROL_FRAUDES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'PRODUCCION1',
      'DEST_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'DEST_DATASET_NAME': CONTROL_FRAUDES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_PRODUCCION1',
    },
    location=CONTROL_FRAUDES_PROJECT_REGION,
    gcp_conn_id=CONTROL_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  dm_produccion2 = BigQueryInsertJobOperator(
    task_id="dm_produccion2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/PRODUCCION2/DM_PRODUCCION2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'SOURCE_DATASET_NAME': CONTROL_FRAUDES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'PRODUCCION2',
      'DEST_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'DEST_DATASET_NAME': CONTROL_FRAUDES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_PRODUCCION2',
    },
    location=CONTROL_FRAUDES_PROJECT_REGION,
    gcp_conn_id=CONTROL_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  
  dm_recuperaciones = BigQueryInsertJobOperator(
    task_id="dm_recuperaciones",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path=f'gs://{DATA_COMPOSER_WORKSPACE_BUCKET_NAME}/workspaces/models/RECUPERACIONES/DM_RECUPERACIONES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'SOURCE_DATASET_NAME': CONTROL_FRAUDES_LAN_DATASET_NAME,
      'SOURCE_TABLE_NAME': 'RECUPERACIONES',
      'DEST_PROJECT_ID': CONTROL_FRAUDES_PROJECT_ID,
      'DEST_DATASET_NAME': CONTROL_FRAUDES_DM_DATASET_NAME,
      'DEST_TABLE_NAME': 'DM_RECUPERACIONES',
    },
    location=CONTROL_FRAUDES_PROJECT_REGION,
    gcp_conn_id=CONTROL_FRAUDES_CONNECTION_DEFAULT,
    deferrable=True,
    poll_interval=30,

    dag=dag 
  )
  

init >> bq_elt()