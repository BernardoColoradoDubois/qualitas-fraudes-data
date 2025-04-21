import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from lib.utils import execute_query_workflow,get_bucket_file_contents
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

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


dm_asegurados = BigQueryInsertJobOperator(
  task_id="dm_asegurados",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ASEGURADOS/DM_ASEGURADOS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'sample_landing_siniestros_bsc',
    'SOURCE_TABLE_NAME': 'maseg_bsc',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_ASEGURADOS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)


dm_causas = BigQueryInsertJobOperator(
  task_id="dm_causas",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/CAUSAS/DM_CAUSAS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'sample_landing_siniestros',
    'SOURCE_TABLE_NAME': 'cat_causa',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_CAUSAS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)


dm_oficinas = BigQueryInsertJobOperator(
  task_id="dm_oficinas",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/OFICINAS/DM_OFICINAS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'sample_landing_siniestros_bsc',
    'SOURCE_TABLE_NAME': 'tsuc_bsc',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_OFICINAS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_proveedores = BigQueryInsertJobOperator(
  task_id="dm_proveedores",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/PROVEEDORES/DM_PROVEEDORES.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'sample_landing_siniestros_bsc',
    'SOURCE_TABLE_NAME': 'prestadores',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_PROVEEDORES',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_siniestros = BigQueryInsertJobOperator(
  task_id="stg_siniestros",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/SINIESTROS/STG_SINIESTROS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'sample_landing_siniestros',
    'SOURCE_TABLE_NAME': 'sas_sinies',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_SINIESTROS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_siniestros = BigQueryInsertJobOperator(
  task_id="dm_siniestros",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/SINIESTROS/DM_SINIESTROS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_SINIESTROS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_etiqueta_siniestro_1 = BigQueryInsertJobOperator(
  task_id="stg_etiqueta_siniestro_1",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_1.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_1',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_etiqueta_siniestro_2 = BigQueryInsertJobOperator(
  task_id="stg_etiqueta_siniestro_2",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_2.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_2',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_etiqueta_siniestro_3 = BigQueryInsertJobOperator(
  task_id="stg_etiqueta_siniestro_3",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/STG_ETIQUETA_SINIESTRO_3.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_ETIQUETA_SINIESTRO_3',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_etiqueta_siniestro = BigQueryInsertJobOperator(
  task_id="dm_etiqueta_siniestro",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ETIQUETA_SINIESTRO/DM_ETIQUETA_SINIESTRO.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_ETIQUETA_SINIESTRO',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_pagos_polizas = BigQueryInsertJobOperator(
  task_id="dm_pagos_polizas",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/PAGOS_POLIZAS/DM_PAGOS_POLIZAS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_PAGOS_POLIZAS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_coberturas_movimientos = BigQueryInsertJobOperator(
  task_id="dm_coberturas_movimientos",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/COBERTURAS_MOVIMIENTOS/DM_COBERTURAS_MOVIMIENTOS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_COBERTURAS_MOVIMIENTOS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_pagos_proveedores = BigQueryInsertJobOperator(
  task_id="dm_pagos_proveedores",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/PAGOS_PROVEEDORES/DM_PAGOS_PROVEEDORES.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_PAGOS_PROVEEDORES',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_analistas = BigQueryInsertJobOperator(
  task_id="dm_analistas",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ANALISTAS/DM_ANALISTAS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_ANALISTAS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_registro = BigQueryInsertJobOperator(
  task_id="dm_registro",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/REGISTRO/DM_REGISTRO.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_REGISTRO',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_polizas_vigentes_1 = BigQueryInsertJobOperator(
  task_id="stg_polizas_vigentes_1",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_1.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_1',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_polizas_vigentes_2 = BigQueryInsertJobOperator(
  task_id="stg_polizas_vigentes_2",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_2.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_2',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_polizas_vigentes = BigQueryInsertJobOperator(
  task_id="dm_polizas_vigentes",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/POLIZAS_VIGENTES/DM_POLIZAS_VIGENTES.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_POLIZAS_VIGENTES',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_estados = BigQueryInsertJobOperator(
  task_id="dm_estados",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/ESTADOS/DM_ESTADOS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_ESTADOS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_tipos_proveedores = BigQueryInsertJobOperator(
  task_id="dm_tipos_proveedores",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/TIPOS_PROVEEDORES/DM_TIPOS_PROVEEDORES.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_TIPOS_PROVEEDORES',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_incisos_polizas_1 = BigQueryInsertJobOperator(
  task_id="stg_incisos_polizas_1",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_1.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_SINIESTROS',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_1',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

stg_incisos_polizas_2 = BigQueryInsertJobOperator(
  task_id="stg_incisos_polizas_2",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_2.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_1',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'STG_FRAUDES',
    'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_2',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_incisos_polizas = BigQueryInsertJobOperator(
  task_id="dm_incisos_polizas",
  configuration={
    "query": {
      "query": get_bucket_file_contents(path='gs://us-central1-ccompquafrau-38b343aa-bucket/workspaces/models/INCISOS_POLIZAS/DM_INCISOS_POLIZAS.sql'),
      "useLegacySql": False,
    }
  },
  params={
    'SOURCE_PROJECT_ID': 'qualitasfraude',
    'SOURCE_DATASET_NAME': 'STG_FRAUDES',
    'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_2',
    'DEST_PROJECT_ID': 'qualitasfraude',
    'DEST_DATASET_NAME': 'DM_FRAUDES',
    'DEST_TABLE_NAME': 'DM_INCISOS_POLIZAS',
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)


init >> dm_causas 
init >> dm_oficinas 
init >> dm_proveedores 
init >> stg_siniestros >> dm_siniestros 
init >> dm_pagos_polizas 
init >> dm_coberturas_movimientos 
init >> dm_pagos_proveedores 
init >> dm_analistas
init >> dm_registro
init >> dm_estados
init >> dm_asegurados
init >> dm_tipos_proveedores
init >> stg_etiqueta_siniestro_1 >> stg_etiqueta_siniestro_2 >> stg_etiqueta_siniestro_3 >> dm_etiqueta_siniestro 
init >> stg_polizas_vigentes_1 >> stg_polizas_vigentes_2 >> dm_polizas_vigentes
init >> stg_incisos_polizas_1 >> stg_incisos_polizas_2 >> dm_incisos_polizas