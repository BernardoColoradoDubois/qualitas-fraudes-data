import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionGetInstanceOperator
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from lib.utils import get_bucket_file_contents

init_date = '2025-03-01'
final_date = '2025-03-31'

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=2)
}

dag = DAG(
  'verificaciones_elt',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

init_elt = BashOperator(task_id='init_elt',bash_command='echo init ELT',dag=dag)


# ASEGURADO
dm_asegurados = BigQueryInsertJobOperator(
  task_id="dm_asegurados",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/ASEGURADOS/DM_ASEGURADOS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'MASEG_BSC',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_ASEGURADOS',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

# PAGOS_PROVEEDORES
rtl_pagos_proveedores = BigQueryInsertJobOperator(
  task_id="rtl_pagos_proveedores",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/PAGOS_PROVEEDORES/RTL_PAGOS_PROVEEDORES.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'PAGOPROVE',
    'SOURCE_SECOND_TABLE_NAME': 'PAGOSPROVEEDORES',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
    'DEST_TABLE_NAME': 'RTL_PAGOS_PROVEEDORES',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_pagos_proveedores = BigQueryInsertJobOperator(
  task_id="dm_pagos_proveedores",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/PAGOS_PROVEEDORES/DM_PAGOS_PROVEEDORES.sql'),
      "useLegacySql": False,
    },
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'RTL_PAGOS_PROVEEDORES',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_PAGOS_PROVEEDORES',
    'init_date':init_date,
    'final_date':final_date
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

# PROVEEDORES
dm_proveedores = BigQueryInsertJobOperator(
  task_id="dm_proveedores",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/PROVEEDORES/DM_PROVEEDORES.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'PRESTADORES',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_PROVEEDORES',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

rtl_coberturas_movimientos = BigQueryInsertJobOperator(
  task_id="rtl_coberturas_movimientos",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/COBERTURAS_MOVIMIENTOS/RTL_COBERTURAS_MOVIMIENTOS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'RESERVAS_BSC',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
    'DEST_TABLE_NAME': 'RTL_COBERTURAS_MOVIMIENTOS'
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_coberturas_movimientos = BigQueryInsertJobOperator(
  task_id="dm_coberturas_movimientos",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/COBERTURAS_MOVIMIENTOS/DM_COBERTURAS_MOVIMIENTOS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'RTL_COBERTURAS_MOVIMIENTOS',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_COBERTURAS_MOVIMIENTOS',
    'init_date':init_date,
    'final_date':final_date
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_estados = BigQueryInsertJobOperator(
  task_id="dm_estados",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/ESTADOS/DM_ESTADOS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'TESTADO_BSC',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_ESTADOS',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_oficinas = BigQueryInsertJobOperator(
  task_id="dm_oficinas",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/OFICINAS/DM_OFICINAS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'TSUC_BSC',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_OFICINAS',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_tipos_proveedores = BigQueryInsertJobOperator(
  task_id="dm_tipos_proveedores",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/TIPOS_PROVEEDORES/DM_TIPOS_PROVEEDORES.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'TIPOPROVEEDOR',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_TIPOS_PROVEEDORES',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_causas = BigQueryInsertJobOperator(
  task_id="dm_causas",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/CAUSAS/DM_CAUSAS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'CAT_CAUSA',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_CAUSAS',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_etiqueta_siniestro_1 = BigQueryInsertJobOperator(
  task_id="stg_etiqueta_siniestro_1",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_1.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'ETIQUETA_SINIESTRO',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
    'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_1',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_etiqueta_siniestro_2 = BigQueryInsertJobOperator(
  task_id="stg_etiqueta_siniestro_2",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_2.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_1',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
    'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_2',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_etiqueta_siniestro_3 = BigQueryInsertJobOperator(
  task_id="stg_etiqueta_siniestro_3",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_3.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_2',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
    'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_3',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

rtl_etiqueta_siniestro = BigQueryInsertJobOperator(
  task_id="rtl_etiqueta_siniestro",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/ETIQUETA_SINIESTRO/RTL_ETIQUETA_SINIESTRO.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_3',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
    'DEST_TABLE_NAME': 'RTL_ETIQUETA_SINIESTRO',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_etiqueta_siniestro = BigQueryInsertJobOperator(
  task_id="dm_etiqueta_siniestro",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/ETIQUETA_SINIESTRO/DM_ETIQUETA_SINIESTRO.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'RTL_ETIQUETA_SINIESTRO',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_ETIQUETA_SINIESTRO',
    'init_date':init_date,
    'final_date':final_date
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

rtl_registro = BigQueryInsertJobOperator(
  task_id="rtl_registro",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/REGISTRO/RTL_REGISTRO.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'REGISTRO',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
    'DEST_TABLE_NAME': 'RTL_REGISTRO',
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_registro = BigQueryInsertJobOperator(
  task_id="dm_registro",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/REGISTRO/DM_REGISTRO.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'RTL_REGISTRO',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_REGISTRO',
    'init_date':init_date,
    'final_date':final_date
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_siniestros = BigQueryInsertJobOperator(
  task_id="stg_siniestros",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/SINIESTROS/STG_SINIESTROS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'SAS_SINIES',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
    'DEST_TABLE_NAME': 'STG_SINIESTROS'
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

rtl_siniestros = BigQueryInsertJobOperator(
  task_id="rtl_siniestros",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/SINIESTROS/RTL_SINIESTROS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
    'DEST_TABLE_NAME': 'RTL_SINIESTROS'
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_siniestros = BigQueryInsertJobOperator(
  task_id="dm_siniestros",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/SINIESTROS/DM_SINIESTROS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
    'SOURCE_TABLE_NAME': 'RTL_SINIESTROS',
    'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
    'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
    'DEST_TABLE_NAME': 'DM_SINIESTROS',
    'init_date':init_date,
    'final_date':final_date
  },
  location='us-central1',
  gcp_conn_id="google_cloud_default",
  dag=dag 
)




end_elt = BashOperator(task_id='end_elt',bash_command='echo end ELT',dag=dag)

init_elt >> dm_asegurados >> end_elt
init_elt >> rtl_pagos_proveedores  >> dm_pagos_proveedores >> end_elt
init_elt >> dm_proveedores >> end_elt
init_elt >> rtl_coberturas_movimientos >> dm_coberturas_movimientos >> end_elt
init_elt >> dm_estados >> end_elt
init_elt >> dm_oficinas >> end_elt
init_elt >> dm_tipos_proveedores >> end_elt
init_elt >> dm_causas >> end_elt
init_elt >> stg_etiqueta_siniestro_1 >> stg_etiqueta_siniestro_2 >> stg_etiqueta_siniestro_3 >> rtl_etiqueta_siniestro >> dm_etiqueta_siniestro >> end_elt
init_elt >> rtl_registro >> dm_registro >> end_elt
init_elt >> stg_siniestros >> rtl_siniestros >> dm_siniestros >> end_elt





