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

from lib.utils import get_bucket_file_contents

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=2)
}

dag = DAG(
  'create_calendar',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

init = BashOperator(task_id='init',bash_command='echo "Iniciando el DAG"',dag=dag)

@task_group(group_id='seed_tables',dag=dag)
def seed_tables():
  
  seed_estados_mexico = BigQueryInsertJobOperator(
    task_id="seed_estados_mexico",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/SEED/DDL_ESTADOS_MEXICO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'SEED_VERIFICACIONES',
      'TABLE_NAME': 'ESTADOS_MEXICO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  
  
@task_group(group_id='landing_tables',dag=dag)
def landing_tables():
  
  lan_analistas = BigQueryInsertJobOperator(
    task_id="lan_analistas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DM_CALENDARIO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'ANALISTAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_apercab = BigQueryInsertJobOperator(
    task_id="lan_apercab",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_APERCAB_BSC.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'APERCAB_BSC',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_cat_causa = BigQueryInsertJobOperator(
    task_id="lan_cat_causa",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_CAT_CAUSA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'DM_CALENDARIO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_cobranza = BigQueryInsertJobOperator(
    task_id="lan_cobranza",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_COBRANZA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'COBRANZA',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_cobranza_hist = BigQueryInsertJobOperator(
    task_id="lan_cobranza_hist",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_COBRANZA_HIST.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'COBRANZA_HIST',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_datos_dua = BigQueryInsertJobOperator(
    task_id="lan_datos_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_DATOS_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'DATOS_DUA',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_etiqueta_siniestro = BigQueryInsertJobOperator(
    task_id="lan_etiqueta_siniestro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_ETIQUETA_SINIESTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'ETIQUETA_SINIESTRO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_frau_pv = BigQueryInsertJobOperator(
    task_id="lan_frau_pv",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_FRAUD_PV.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'FRAUD_PV',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_frau_di = BigQueryInsertJobOperator(
    task_id="lan_frau_di",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_FRAUD_DI.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'FRAUD_DI',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_frau_rp = BigQueryInsertJobOperator(
    task_id="lan_frau_rp",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_FRAUD_RP.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'FRAUD_RP',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  
  lan_maseg_bsc = BigQueryInsertJobOperator(
    task_id="lan_maseg_bsc",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_MASEG_BSC.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'MASEG_BSC',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_pagoprove = BigQueryInsertJobOperator(
    task_id="lan_pagoprove",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_PAGOPROVE.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'PAGOPROVE',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_pagosproveedores = BigQueryInsertJobOperator(
    task_id="lan_pagosproveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_PAGOSPROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'PAGOSPROVEEDORES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_prestadores = BigQueryInsertJobOperator(
    task_id="lan_prestadores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_PRESTADORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'PRESTADORES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_reservas_bsc = BigQueryInsertJobOperator(
    task_id="lan_reservas_bsc",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_RESERVAS_BSC.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'RESERVAS_BSC',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_sas_sinies = BigQueryInsertJobOperator(
    task_id="lan_sas_sinies",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_SAS_SINIES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'SAS_SINIES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_testado = BigQueryInsertJobOperator(
    task_id="lan_testado",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_TESTADO_BSC.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'TESTADO_BSC',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_tipoproveedor = BigQueryInsertJobOperator(
    task_id="lan_tipoproveedor",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_TIPOPROVEEDOR.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'TIPOPROVEEDOR',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  lan_tsuc_bsc = BigQueryInsertJobOperator(
    task_id="lan_tsuc_bsc",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/LAN/DDL_TSUC_BSC.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'LAN_VERIFICACIONES',
      'TABLE_NAME': 'TSUC_BSC',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )



@task_group(group_id='staging_tables',dag=dag)
def staging_tables():
  
  stg_dua = BigQueryInsertJobOperator(
    task_id="stg_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_DUA',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_etiqueta_siniestro_1 = BigQueryInsertJobOperator(
    task_id="stg_etiqueta_siniestro_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_ETIQUETA_SINIESTRO_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_1',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_etiqueta_siniestro_2 = BigQueryInsertJobOperator(
    task_id="stg_etiqueta_siniestro_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_ETIQUETA_SINIESTRO_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_2',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_etiqueta_siniestro_3 = BigQueryInsertJobOperator(
    task_id="stg_etiqueta_siniestro_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_ETIQUETA_SINIESTRO_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_3',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_incisos_polizas_1 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_INCISOS_POLIZAS_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_INCISOS_POLIZAS_1',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_incisos_polizas_2 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_INCISOS_POLIZAS_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_INCISOS_POLIZAS_2',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_incisos_polizas_3 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_INCISOS_POLIZAS_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_INCISOS_POLIZAS_3',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_incisos_polizas_4 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_4",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_INCISOS_POLIZAS_4.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_INCISOS_POLIZAS_4',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_pagos_polizas = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_PAGOS_POLIZAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_polizas_vigentes_1 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_POLIZAS_VIGENTES_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_POLIZAS_VIGENTES_1',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_polizas_vigentes_2 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_POLIZAS_VIGENTES_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_POLIZAS_VIGENTES_2',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_polizas_vigentes_3 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_POLIZAS_VIGENTES_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_POLIZAS_VIGENTES_3',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_polizas_vigentes_4 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_4",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_POLIZAS_VIGENTES_4.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_POLIZAS_VIGENTES_4',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_registro = BigQueryInsertJobOperator(
    task_id="stg_registro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_REGISTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_REGISTRO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_siniestros = BigQueryInsertJobOperator(
    task_id="stg_siniestros",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/STG/DDL_STG_SINIESTROS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'STG_VERIFICACIONES',
      'TABLE_NAME': 'STG_SINIESTROS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )


@task_group(group_id='ready_to_load_tables',dag=dag)
def ready_to_load_tables():

  rtl_coberturas = BigQueryInsertJobOperator(
    task_id="rtl_coberturas_movimientos",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_COBERTURAS_MOVIMIENTOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_COBERTURAS_MOVIMIENTOS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  rtl_dua = BigQueryInsertJobOperator(
    task_id="rtl_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_DUA',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  rtl_etiqueta_siniestro = BigQueryInsertJobOperator(
    task_id="rtl_etiqueta_siniestro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_ETIQUETA_SINIESTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_ETIQUETA_SINIESTRO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  rtl_incisos_polizas = BigQueryInsertJobOperator(
    task_id="rtl_incisos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_INCISOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_INCISOS_POLIZAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  rtl_pagos_polizas = BigQueryInsertJobOperator(
    task_id="rtl_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_PAGOS_POLIZAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  rtl_pagos_proveedores = BigQueryInsertJobOperator(
    task_id="rtl_pagos_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_PAGOS_PROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_PAGOS_PROVEEDORES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  rtl_polizas_vigentes = BigQueryInsertJobOperator(
    task_id="rtl_polizas_vigentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_POLIZAS_VIGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_POLIZAS_VIGENTES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  rtl_registro = BigQueryInsertJobOperator(
    task_id="rtl_registro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_REGISTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_REGISTRO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  rtl_siniestros = BigQueryInsertJobOperator(
    task_id="rtl_siniestros",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/RTL/DDL_RTL_SINIESTROS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'RTL_VERIFICACIONES',
      'TABLE_NAME': 'RTL_SINIESTROS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
@task_group(group_id='data_mart_tables',dag=dag)
def data_mart_tables():
  
  dm_calendario = BigQueryInsertJobOperator(
    task_id="dm_calendario",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_CALENDARIO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_CALENDARIO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_asegurados = BigQueryInsertJobOperator(
    task_id="dm_asegurados",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_ASEGURADOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_ASEGURADOS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_causas = BigQueryInsertJobOperator(
    task_id="rtl_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_CAUSAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_CAUSAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_coberturas_movimientos = BigQueryInsertJobOperator(
    task_id="dm_coberturas_movimientos",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DDL_DM_COBERTURAS_MOVIMIENTOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_COBERTURAS_MOVIMIENTOS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  

  dm_dua = BigQueryInsertJobOperator(
    task_id="dm_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_DUA',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_estados = BigQueryInsertJobOperator(
    task_id="dm_estados",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_ESTADOS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_ESTADOS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_etiqueta_siniestro = BigQueryInsertJobOperator(
    task_id="dm_etiqueta_siniestro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_ETIQUETA_SINIESTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_ETIQUETA_SINIESTRO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_incisos_polizas = BigQueryInsertJobOperator(
    task_id="dm_incisos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_INCISOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_INCISOS_POLIZAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_oficinas = BigQueryInsertJobOperator(
    task_id="dm_oficinas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_OFICINAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_OFICINAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_pagos_polizas = BigQueryInsertJobOperator(
    task_id="dm_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_PAGOS_POLIZAS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_pagos_proveedores = BigQueryInsertJobOperator(
    task_id="dm_pagos_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_PAGOS_PROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_PAGOS_PROVEEDORES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_polizas_vigentes = BigQueryInsertJobOperator(
    task_id="dm_polizas_vigentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_POLIZAS_VIGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_POLIZAS_VIGENTES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_proveedores = BigQueryInsertJobOperator(
    task_id="dm_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_PROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_PROVEEDORES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_registro = BigQueryInsertJobOperator(
    task_id="dm_registro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_REGISTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_REGISTRO',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  dm_siniestros = BigQueryInsertJobOperator(
    task_id="dm_siniestros",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_SINIESTROS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_SINIESTROS',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_tipos_proveedores = BigQueryInsertJobOperator(
    task_id="dm_tipos_proveedores",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/ddls/DM/DDL_DM_TIPOS_PROVEEDORES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DATASET_NAME': 'DM_VERIFICACIONES',
      'TABLE_NAME': 'DM_TIPOS_PROVEEDORES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

end = BashOperator(task_id='end',bash_command='echo "Finalizando el DAG"',dag=dag,)
init >> [seed_tables(), landing_tables(),staging_tables(),ready_to_load_tables(),data_mart_tables() ] >> end