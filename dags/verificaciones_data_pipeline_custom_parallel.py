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
    "num_instances": 18,
     "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "secondary_worker_config": {
    "num_instances": 6,
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
      
      # Configuración optimizada para Spark
      "spark:spark.executor.cores": "1",                    # 1 core por executor para maximizar paralelismo
      "spark:spark.executor.memory": "2g",                  # Memoria por executor
      "spark:spark.driver.memory": "4g",                    # Memoria para el driver
      "spark:spark.executor.instances": "42",               # Incrementado significativamente (7 pipelines * 6)
      "spark:spark.yarn.am.memory": "1g",                   # Memoria para YARN Application Master
      
      # Configuraciones de asignación dinámica
      "spark:spark.dynamicAllocation.enabled": "true",      # Habilitado
      "spark:spark.dynamicAllocation.minExecutors": "2",    # Mínimo
      "spark:spark.dynamicAllocation.maxExecutors": "48",   # Máximo (aumentado significativamente)
      "spark:spark.dynamicAllocation.initialExecutors": "21", # Inicial (7 pipelines * 3)
      
      # Configuraciones de programación y rendimiento
      "spark:spark.scheduler.mode": "FAIR",                 # Modo justo
      "spark:spark.task.maxFailures": "8",                  # Tolerancia a fallos
      "spark:spark.stage.maxConsecutiveAttempts": "4",      # Reintentos
      "spark:spark.locality.wait": "0s",                    # No esperar por localidad para maximizar paralelismo
      
      # Configuraciones de memoria y rendimiento
      "spark:spark.yarn.executor.memoryOverhead": "512m",   # Overhead de memoria
      "spark:spark.shuffle.service.enabled": "true",        # Habilitar servicio de shuffle
      "spark:spark.network.timeout": "800s",                # Aumentar timeout de red
      "spark:spark.speculation": "true",                    # Habilitar especulación
      "spark:spark.speculation.quantile": "0.75",           # Umbral para especulación
      
      # Optimizaciones de SQL y particionamiento
      "spark:spark.sql.shuffle.partitions": "32",           # Particiones para shuffle (ajustar según necesidad)
      "spark:spark.default.parallelism": "32",              # Paralelismo predeterminado
      
      # Optimizaciones de GC
      "spark:spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35",
      
      # Optimizaciones YARN
      "yarn:yarn.scheduler.capacity.maximum-am-resource-percent": "0.5", # Aumentar recursos para AMs
      "yarn:yarn.nodemanager.resource.memory-mb": "7168",   # Memoria por nodo worker
      "yarn:yarn.scheduler.maximum-allocation-mb": "6144",  # Asignación máxima
      "yarn:yarn.nodemanager.vmem-check-enabled": "false",  # Desactivar verificación de memoria virtual
      
      # Optimizaciones de MapReduce (backend)
      "mapred:mapreduce.map.speculative": "true",          # Especulación para MapReduce
      "mapred:mapreduce.reduce.speculative": "true"  
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
  'retry_delay': timedelta(minutes=10)
}

dag = DAG(
  'verificaciones_data_pipeline_dataproc_custom_parallel',
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
  pipeline_timeout=3600,
  asynchronous=False,
  deferrable=True,
  poll_interval=30,
  runtime_args={
    'system.runtime.args.spark.executor.instances': '7',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '12',
    'system.runtime.args.spark.sql.shuffle.partitions': '24',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",
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
    'system.runtime.args.spark.executor.instances': '6',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '10',
    'system.runtime.args.spark.sql.shuffle.partitions': '16',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",
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
    'system.runtime.args.spark.executor.instances': '7',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '12',
    'system.runtime.args.spark.sql.shuffle.partitions': '24',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PAGOPROVE',
    'init_date':init_date, 
    'final_date':final_date
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
    'system.runtime.args.spark.executor.instances': '7',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '12',
    'system.runtime.args.spark.sql.shuffle.partitions': '24',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",    
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PAGOSPROVEEDORES',
    'init_date':init_date, 
    'final_date':final_date
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
    'system.runtime.args.spark.executor.instances': '6',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '10',
    'system.runtime.args.spark.sql.shuffle.partitions': '16',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",        
    'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
    'DATASET_NAME':'LAN_VERIFICACIONES',
    'TABLE_NAME':'PRESTADORES'
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
    'system.runtime.args.spark.executor.instances': '7',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '12',
    'system.runtime.args.spark.sql.shuffle.partitions': '24',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",        
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
    'system.runtime.args.spark.executor.instances': '4',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '8',
    'system.runtime.args.spark.sql.shuffle.partitions': '8',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",  
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
    'system.runtime.args.spark.executor.instances': '4',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '8',
    'system.runtime.args.spark.sql.shuffle.partitions': '8',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",  
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
    'system.runtime.args.spark.executor.instances': '4',
    'system.runtime.args.spark.dynamicAllocation.maxExecutors': '8',
    'system.runtime.args.spark.sql.shuffle.partitions': '8',
    'dataproc.cluster.name':'verificaciones-dataproc',
    "system.profile.name" : "USER:verificaciones-dataproc",  
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

delete_cluster = DataprocDeleteClusterOperator(
  task_id="delete_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_name="verificaciones-dataproc",
  region="us-central1",
)

create_cluster >> init_landing >> get_datafusion_instance >> init_landing_bsc_siniestros
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
end_landing >> delete_cluster




