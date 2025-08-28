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



VERIFICACIONES_ENV_CONFIG	 = Variable.get("VERIFICACIONES_ENV_CONFIG", deserialize_json=True)


ENV_VARIABLE = VERIFICACIONES_ENV_CONFIG['ENV']

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


@task_group(group_id='bro_datasets',dag=dag)
def create_datasets_bro():
  
  print(f'Creating the following dataset: qlts-{ENV_VARIABLE}-mx-au-bro-verificacio.qlts_bro_op_verificaciones_{ENV_VARIABLE}')
  dataset_bro = BigQueryInsertJobOperator(
    task_id="dataset_bro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/DATASETS/BRO_DATASET.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'qlts-{ENV_VARIABLE}-mx-au-bro-verificacio',
      'DATASET_NAME': f'qlts_bro_op_verificaciones_{ENV_VARIABLE}',
      'LOCATION': 'us-central1',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )


@task_group(group_id='pla_datasets',dag=dag)
def create_datasets_pla():
  dataset_pla = BigQueryInsertJobOperator(
    task_id="dataset_pla",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/DATASETS/PLA_DATASET.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'qlts-{ENV_VARIABLE}-mx-au-pla-verificacio',
      'DATASET_NAME': f'qlts_pla_op_verificaciones_{ENV_VARIABLE}',
      'LOCATION': 'us-central1',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )




@task_group(group_id='oro_datasets',dag=dag)
def create_datasets_oro():
  
  dataset_oro = BigQueryInsertJobOperator(
    task_id="dataset_oro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/DATASETS/ORO_DATASET.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'PROJECT_ID': f'qlts-{ENV_VARIABLE}-mx-au-oro-verificacio',
      'DATASET_NAME': f'qlts_oro_op_verificaciones_{ENV_VARIABLE}',
      'LOCATION': 'us-central1',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  
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
init >> [create_datasets_bro(), create_datasets_pla(), create_datasets_oro() ] >> end