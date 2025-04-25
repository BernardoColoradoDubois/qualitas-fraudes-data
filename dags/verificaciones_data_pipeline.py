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
  'verificaciones_data_pipeline',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

init_landing = BashOperator(task_id='init_landing',bash_command='echo init landing',dag=dag)

get_datafusion_instance = CloudDataFusionGetInstanceOperator(
  task_id="get_datafusion_instance",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  project_id='qlts-nonprod-data-tools',
  dag=dag,
)

init_landing_bsc_siniestros = BashOperator(task_id='init_landing_bsc_siniestros',bash_command='echo init landing BSCSiniestros',dag=dag)


# apercab pipeline
load_apercab_bsc = CloudDataFusionStartPipelineOperator(
  task_id="load_apercab_bsc",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_apercab_bsc',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'APERCAB_BSC',
    'init_date':init_date, 
    'final_date':final_date
  },
  dag=dag
)

# maseg pipeline
load_maseg_bsc = CloudDataFusionStartPipelineOperator(
  task_id="load_maseg_bsc",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_maseg',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'MASEG_BSC'
  },
  dag=dag
)

# pagoprove pipeline
load_pagoprove = CloudDataFusionStartPipelineOperator(
  task_id="load_pagoprove",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_pagoprove',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PAGOPROVE',
    'init_date':init_date, 
    'final_date':final_date
  },
  dag=dag
)

# pagosproveedores pipeline
load_pagosproveedores = CloudDataFusionStartPipelineOperator(
  task_id="load_pagosproveedores",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_pagosproveedores',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PAGOSPROVEEDORES',
    'init_date':init_date, 
    'final_date':final_date
  },
  dag=dag
)

# prestadores pipeline
load_prestadores = CloudDataFusionStartPipelineOperator(
  task_id="load_prestadores",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_prestadores',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PRESTADORES'
  },
  dag=dag
)

# reservas_bsc pipeline
load_reservas_bsc = CloudDataFusionStartPipelineOperator(
  task_id="load_reservas_bsc",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_reservas',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'RESERVAS_BSC',
    'init_date':init_date, 
    'final_date':final_date
  },
  dag=dag
)

# testado_bsc pipeline
load_testado_bsc = CloudDataFusionStartPipelineOperator(
  task_id="load_testado_bsc",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_testados_bsc',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'TESTADO_BSC'
  },
  dag=dag
)

# tipoproveedor pipeline
load_tipoproveedor = CloudDataFusionStartPipelineOperator(
  task_id="load_tipoproveedor",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='qlts_dev_verificaciones_tipoproveedor',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'TIPOPROVEEDOR',
  },
  dag=dag
)

# tsuc pipeline
load_tsuc_bsc = CloudDataFusionStartPipelineOperator(
  task_id="load_tsuc_bsc",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='carga_qlts_dev_verificaciones_tsuc',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'TSUC_BSC',
    'init_date':init_date, 
    'final_date':final_date
  },
  dag=dag
)

end_landing_bsc_siniestros = BashOperator(task_id='end_landing_bsc_siniestros',bash_command='echo end landing BSCSiniestros',dag=dag)

end_landing = BashOperator(task_id='end_landing',bash_command='echo end landing',dag=dag)

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

end_elt = BashOperator(task_id='end_elt',bash_command='echo end ELT',dag=dag)

init_injection = BashOperator(task_id='init_injection',bash_command='echo init inyection',dag=dag)


init_landing >> get_datafusion_instance >> init_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_apercab_bsc >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_maseg_bsc >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_pagoprove >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_pagosproveedores >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_prestadores >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_reservas_bsc >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_testado_bsc >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_tipoproveedor >> end_landing_bsc_siniestros
init_landing_bsc_siniestros >> load_tsuc_bsc >> end_landing_bsc_siniestros
end_landing_bsc_siniestros >> end_landing
end_landing >> init_elt

init_elt >> dm_asegurados >> end_elt
init_elt >> rtl_pagos_proveedores  >> dm_pagos_proveedores >> end_elt
init_elt >> dm_proveedores >> end_elt
init_elt >> rtl_coberturas_movimientos >> dm_coberturas_movimientos >> end_elt
init_elt >> dm_estados >> end_elt
init_elt >> dm_oficinas >> end_elt
init_elt >> dm_tipos_proveedores >> end_elt

end_elt>> init_injection


