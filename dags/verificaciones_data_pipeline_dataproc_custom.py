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
    "num_instances": 12,
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
    "preemptibility": "PREEMPTIBLE",
  },
  "software_config": {
  "image_version":"2.1.85-debian11",
  "properties": {
    "dataproc:dataproc.conscrypt.provider.enable": "false",
    "capacity-scheduler:yarn.scheduler.capacity.resource-calculator":"org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
    "spark:spark.executor.cores": "2",                     # Reducir de 10 a 2
    "spark:spark.executor.memory": "3g",                   # Añadir configuración de memoria
    "spark:spark.driver.memory": "4g",                     # Añadir memoria para el driver
    "spark:spark.executor.instances": "4",                 # Controlar número de executors
    "spark:spark.yarn.am.memory": "1g",                    # Memoria para YARN Application Master
    "spark:spark.dynamicAllocation.enabled": "true",       # Habilitar asignación dinámica
    "spark:spark.dynamicAllocation.minExecutors": "2",     # Mínimo de executors
    "spark:spark.dynamicAllocation.maxExecutors": "8",     # Máximo de executors
    "spark:spark.scheduler.mode": "FAIR"                   # Programador justo
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
  'retries': 4,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG(
  'verificaciones_data_pipeline_dataproc_custom',
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

init_landing = BashOperator(task_id='init_landing',bash_command='echo init landing',dag=dag)

get_datafusion_instance = CloudDataFusionGetInstanceOperator(
  task_id="get_datafusion_instance",
  location='us-central1',
  instance_name='qlts-data-fusion-dev',
  project_id='qlts-nonprod-data-tools',
  dag=dag,
)

init_landing_bsc_siniestros_1 = BashOperator(task_id='init_landing_bsc_siniestros_1',bash_command='echo init landing BSCSiniestros',dag=dag)

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
  pipeline_timeout=3600,
  asynchronous=False,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'APERCAB_BSC',
    'init_date':init_date, 
    'final_date':final_date,
    'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',

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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'MASEG_BSC',
    'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PAGOPROVE',
    'init_date':init_date, 
    'final_date':final_date,
        'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
  },
  dag=dag
)

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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PAGOSPROVEEDORES',
    'init_date':init_date, 
    'final_date':final_date,
        'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
  },
  dag=dag
)

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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",        
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PRESTADORES',
        'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
  },
  dag=dag
)

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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",        
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'RESERVAS_BSC',
    'init_date':init_date, 
    'final_date':final_date,
        'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",  
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'TESTADO_BSC',
        'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",  
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'TIPOPROVEEDOR',
        'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
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
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",  
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'TSUC_BSC',
    'init_date':init_date, 
    'final_date':final_date,
        'spark.driver.memory': '2g',
    'spark.executor.memory': '3g',
    'spark.executor.cores': '2',
    'spark.executor.instances': '4',
    'system.spark.driver.memory': '2g',
    'system.spark.executor.memory': '3g',
  },
  dag=dag
)


init_landing_bsc_siniestros_2 = BashOperator(task_id='init_landing_bsc_siniestros_2',bash_command='echo end landing BSCSiniestros',dag=dag)
init_landing_bsc_siniestros_3 = BashOperator(task_id='init_landing_bsc_siniestros_3',bash_command='echo end landing BSCSiniestros',dag=dag)

end_landing_bsc_siniestros = BashOperator(task_id='end_landing_bsc_siniestros',bash_command='echo end landing',dag=dag)


delete_cluster = DataprocDeleteClusterOperator(
  task_id="delete_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_name="verificaciones-dataproc",
  region="us-central1",
)

create_cluster >> init_landing >> get_datafusion_instance >> init_landing_bsc_siniestros_1
init_landing_bsc_siniestros_1 >> load_apercab_bsc >> init_landing_bsc_siniestros_2
init_landing_bsc_siniestros_1 >> load_maseg_bsc >> init_landing_bsc_siniestros_2
init_landing_bsc_siniestros_1 >> load_pagoprove >> init_landing_bsc_siniestros_2

init_landing_bsc_siniestros_2 >> load_pagosproveedores >> init_landing_bsc_siniestros_3
init_landing_bsc_siniestros_2 >> load_prestadores >> init_landing_bsc_siniestros_3
init_landing_bsc_siniestros_2 >> load_reservas_bsc >> init_landing_bsc_siniestros_3

init_landing_bsc_siniestros_3 >> load_testado_bsc >> end_landing_bsc_siniestros
init_landing_bsc_siniestros_3 >> load_tipoproveedor >> end_landing_bsc_siniestros
init_landing_bsc_siniestros_3 >> load_tsuc_bsc >> end_landing_bsc_siniestros

# init_landing_bsc_siniestros_2 >> end_landing_bsc_siniestros
end_landing_bsc_siniestros >> delete_cluster




