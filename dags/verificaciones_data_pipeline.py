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

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG(
  'verificaciones_data_pipeline',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=40),
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
    'init_date':'2023-01-01', 
    'final_date':'2023-01-31'
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
    'init_date':'2023-01-01', 
    'final_date':'2023-01-31'
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
    'init_date':'2023-01-01', 
    'final_date':'2023-01-31'
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
    'init_date':'2023-01-01', 
    'final_date':'2023-01-31'
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
    'init_date':'2023-01-01', 
    'final_date':'2023-01-31'
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
      "query": "SELECT CURRENT_DATE()",
      "useLegacySql": False,
    }
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

# PAGOS_PROVEEDORES
rtl_pagos_proveedores = BigQueryInsertJobOperator(
  task_id="rtl_pagos_proveedores",
  configuration={
    "query": {
      "query": "SELECT CURRENT_DATE()",
      "useLegacySql": False,
    }
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

delete_pagos_proveedores = BigQueryInsertJobOperator(
  task_id="delete_pagos_proveedores",
  configuration={
    "query": {
      "query": "SELECT CURRENT_DATE()",
      "useLegacySql": False,
    }
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

dm_pagos_proveedores = BigQueryInsertJobOperator(
  task_id="dm_pagos_proveedores",
  configuration={
    "query": {
      "query": "SELECT CURRENT_DATE()",
      "useLegacySql": False,
    }
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)

# PROVEEDORES
dm_proveedores = BigQueryInsertJobOperator(
  task_id="dm_prestadores",
  configuration={
    "query": {
      "query": "SELECT CURRENT_DATE()",
      "useLegacySql": False,
    }
  },
  location="US",
  gcp_conn_id="google_cloud_default",
  dag=dag 
)


end_elt = BashOperator(task_id='end_elt',bash_command='echo end ELT',dag=dag)

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
init_elt >> rtl_pagos_proveedores >> delete_pagos_proveedores >> dm_pagos_proveedores >> end_elt
init_elt >> dm_proveedores >> end_elt

