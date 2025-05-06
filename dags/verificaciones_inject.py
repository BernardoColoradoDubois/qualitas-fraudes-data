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

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from lib.utils import get_bucket_file_contents

CLUSTER_CONFIG = {
  "gce_cluster_config": {
    "internal_ip_only": True,
    "subnetwork_uri": "projects/shared-nonprod-eba6/regions/us-central1/subnetworks/qlts-svpc-non-prd-sn",
    "service_account": "dataproc-dev-operaciones@qlts-nonprod-data-tools.iam.gserviceaccount.com",
    "shielded_instance_config": {
      "enable_secure_boot": False,
      "enable_vtpm": False,
      "enable_integrity_monitoring": False,
    }
  },
  "master_config": {
    "num_instances": 1,
    "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "worker_config": {
    "num_instances": 16,
     "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "secondary_worker_config": {
    "num_instances": 4,
    "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard",
      "boot_disk_size_gb": 32,
    },
    "is_preemptible": False,
  },
  "software_config": {
    "image_version":"2.1.85-debian11",
    "properties": {
      "dataproc:dataproc.conscrypt.provider.enable": "false",
      "capacity-scheduler:yarn.scheduler.capacity.resource-calculator":"org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
      "spark:spark.executor.cores": "1",                     # Reducir para tener más executors
      "spark:spark.executor.memory": "2g",                   # Ajustar memoria
      "spark:spark.driver.memory": "4g",                     # Mantener
      "spark:spark.executor.instances": "6",                 # Aumentar
      "spark:spark.yarn.am.memory": "1g",                   
      "spark:spark.dynamicAllocation.enabled": "true",      
      "spark:spark.dynamicAllocation.minExecutors": "2",    
      "spark:spark.dynamicAllocation.maxExecutors": "20",    # Aumentar
      "spark:spark.scheduler.mode": "FAIR",                  
      "spark:spark.task.maxFailures": "8",
      "spark:spark.stage.maxConsecutiveAttempts": "4",
      "spark:spark.locality.wait": "10s",                   # Nuevo
      "spark:spark.shuffle.service.enabled": "true" ,         # Nuevo
      "spark:spark.yarn.executor.memoryOverhead": "512m",  # Ajustar overhead memory
    }
  },
  "endpoint_config": {
    "enable_http_port_access": True
  }
}

init_date = '2025-03-01'
final_date = '2025-03-31'

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 3,
  'retry_delay': timedelta(minutes=10)
}

dag = DAG(
  'verificaciones_inject',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

create_cluster = DataprocCreateClusterOperator(
  task_id="create_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_config=CLUSTER_CONFIG,
  region="us-central1",
  cluster_name="verificaciones-dataproc",
  num_retries_if_resource_is_not_ready=3,
)


get_datafusion_instance = CloudDataFusionGetInstanceOperator(
  task_id="get_datafusion_instance",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  project_id='qlts-nonprod-data-tools',
  dag=dag,
)

init_injection = BashOperator(task_id='init_injection',bash_command='echo init inyection',dag=dag)

# maseg pipeline
inject_dm_asegurados = CloudDataFusionStartPipelineOperator(
  task_id="inject_dm_asegurados",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='inyect_dm_asegurados',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.executor.memory': '3g',         # Formato correcto para Data Fusion
    'system.runtime.args.driver.memory': '2g',           # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.cores': '1',     # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.instances': '4',  # Formato correcto para Data Fusion
    'system.runtime.args.spark.dynamicAllocation.enabled': 'true',
    'system.runtime.args.spark.dynamicAllocation.minExecutors': '2',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '20',
    'system.runtime.args.spark.yarn.am.memory': '1g',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'DM_VERIFICACIONES',
    'TABLE_NAME':'DM_ASEGURADOS',
    'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
    'INJECT_TABLE_NAME':'STG_ASEGURADOS',
  },
  dag=dag
)

inject_coberturas_movimientos = CloudDataFusionStartPipelineOperator(
  task_id="inject_coberturas_movimientos",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='inyect_dm_coberturas_movimientos',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.executor.memory': '3g',         # Formato correcto para Data Fusion
    'system.runtime.args.driver.memory': '2g',           # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.cores': '1',     # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.instances': '4',  # Formato correcto para Data Fusion
    'system.runtime.args.spark.dynamicAllocation.enabled': 'true',
    'system.runtime.args.spark.dynamicAllocation.minExecutors': '2',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '20',
    'system.runtime.args.spark.yarn.am.memory': '1g',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'DM_VERIFICACIONES',
    'TABLE_NAME':'DM_COBERTURAS_MOVIMIENTOS',
    'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
    'INJECT_TABLE_NAME':'STG_COBERTURAS_MOVIMIENTOS',
    'init_date':init_date,
    'final_date':final_date
  },
  dag=dag
)

inject_dm_estados = CloudDataFusionStartPipelineOperator(
  task_id="inject_dm_estados",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='inyect_dm_estados',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.executor.memory': '3g',         # Formato correcto para Data Fusion
    'system.runtime.args.driver.memory': '2g',           # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.cores': '1',     # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.instances': '4',  # Formato correcto para Data Fusion
    'system.runtime.args.spark.dynamicAllocation.enabled': 'true',
    'system.runtime.args.spark.dynamicAllocation.minExecutors': '2',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '20',
    'system.runtime.args.spark.yarn.am.memory': '1g',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'DM_VERIFICACIONES',
    'TABLE_NAME':'DM_ESTADOS',
    'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
    'INJECT_TABLE_NAME':'STG_ESTADOS',
  },
  dag=dag
)

inject_dm_oficinas = CloudDataFusionStartPipelineOperator(
  task_id="inject_dm_oficinas",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='inject_dm_oficinas',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.executor.memory': '3g',         # Formato correcto para Data Fusion
    'system.runtime.args.driver.memory': '2g',           # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.cores': '1',     # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.instances': '4',  # Formato correcto para Data Fusion
    'system.runtime.args.spark.dynamicAllocation.enabled': 'true',
    'system.runtime.args.spark.dynamicAllocation.minExecutors': '2',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '20',
    'system.runtime.args.spark.yarn.am.memory': '1g',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'DM_VERIFICACIONES',
    'TABLE_NAME':'DM_OFICINAS',
    'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
    'INJECT_TABLE_NAME':'STG_OFICINAS',
  },
  dag=dag
)

inject_pagos_proveedores = CloudDataFusionStartPipelineOperator(
  task_id="inject_pagos_proveedores",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='inyect_dm_pagos_proveedores',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.executor.memory': '3g',         # Formato correcto para Data Fusion
    'system.runtime.args.driver.memory': '2g',           # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.cores': '1',     # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.instances': '4',  # Formato correcto para Data Fusion
    'system.runtime.args.spark.dynamicAllocation.enabled': 'true',
    'system.runtime.args.spark.dynamicAllocation.minExecutors': '2',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '20',
    'system.runtime.args.spark.yarn.am.memory': '1g',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'DM_VERIFICACIONES',
    'TABLE_NAME':'DM_PAGOS_PROVEEDORES',
    'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
    'INJECT_TABLE_NAME':'STG_PAGOS_PROVEEDORES',
    'init_date':init_date,
    'final_date':final_date
  },
  dag=dag
)

inject_proveedores = CloudDataFusionStartPipelineOperator(
  task_id="inject_proveedores",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='inject_dm_proveedores',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.executor.memory': '3g',         # Formato correcto para Data Fusion
    'system.runtime.args.driver.memory': '2g',           # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.cores': '1',     # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.instances': '4',  # Formato correcto para Data Fusion
    'system.runtime.args.spark.dynamicAllocation.enabled': 'true',
    'system.runtime.args.spark.dynamicAllocation.minExecutors': '2',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '20',
    'system.runtime.args.spark.yarn.am.memory': '1g',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'DM_VERIFICACIONES',
    'TABLE_NAME':'DM_PROVEEDORES',
    'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
    'INJECT_TABLE_NAME':'STG_PROVEEDORES',
  },
  dag=dag
)

inject_tipos_proveedores = CloudDataFusionStartPipelineOperator(
  task_id="inject_tipos_proveedores",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  namespace='verificaciones',
  pipeline_name='inyect_dm_tipos_proveedores',
  project_id='qlts-nonprod-data-tools',
  pipeline_type = DataFusionPipelineType.BATCH,
  success_states=["COMPLETED"],
  asynchronous=False,
  pipeline_timeout=3600,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.executor.memory': '3g',         # Formato correcto para Data Fusion
    'system.runtime.args.driver.memory': '2g',           # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.cores': '1',     # Formato correcto para Data Fusion
    'system.runtime.args.spark.executor.instances': '4',  # Formato correcto para Data Fusion
    'system.runtime.args.spark.dynamicAllocation.enabled': 'true',
    'system.runtime.args.spark.dynamicAllocation.minExecutors': '2',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '20',
    'system.runtime.args.spark.yarn.am.memory': '1g',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'DM_VERIFICACIONES',
    'TABLE_NAME':'DM_TIPOS_PROVEEDORES',
    'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
    'INJECT_TABLE_NAME':'STG_TIPOS_PROVEEDORES',
  },
  dag=dag
)

end_injection = BashOperator(task_id='end_injection',bash_command='echo end injection',dag=dag)


delete_cluster = DataprocDeleteClusterOperator(
  task_id="delete_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_name="verificaciones-dataproc",
  region="us-central1",
)

create_cluster >> get_datafusion_instance >> init_injection
init_injection >> inject_dm_asegurados >> end_injection
init_injection >> inject_coberturas_movimientos >> end_injection
init_injection >> inject_dm_estados >> end_injection
init_injection >> inject_dm_oficinas >> end_injection
init_injection >> inject_pagos_proveedores >> end_injection
init_injection >> inject_proveedores >> end_injection
init_injection >> inject_tipos_proveedores >> end_injection
end_injection >> delete_cluster





