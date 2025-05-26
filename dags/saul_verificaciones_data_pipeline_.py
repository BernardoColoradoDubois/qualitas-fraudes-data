import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from airflow.models import Variable

from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionGetInstanceOperator
from airflow.providers.google.cloud.operators.datafusion import DataFusionPipelineType 

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from lib.utils import get_bucket_file_contents,get_date_interval,get_cluster_tipe_creator

VERIFICACIONES_LOAD_INTERVAL = Variable.get("VERIFICACIONES_LOAD_INTERVAL", default_var="YESTERDAY")

interval = get_date_interval(project_id='qlts-dev-mx-au-bro-verificacio',period=VERIFICACIONES_LOAD_INTERVAL)

init_date = interval['init_date']
final_date = interval['final_date']


def get_datafusion_runtime_args(table_name, include_dates=True):
    """
    Generate standardized runtime arguments for DataFusion pipelines
    
    Args:
        table_name: Target BigQuery table name
        include_dates: Whether to include init_date and final_date (default: True)
    """
    base_args = {
        'app.pipeline.overwriteConfig': 'true',
        'task.executor.system.resources.cores': '2',
        'task.executor.system.resources.memory': '16g',
        'dataproc.cluster.name': 'verificaciones-dataproc',
        'system.profile.name': 'USER:verificaciones-dataproc',
        'TEMPORARY_BUCKET_NAME': 'gcs-qlts-dev-mx-au-bro-verificaciones',
        'DATASET_NAME': 'LAN_VERIFICACIONES',
        'TABLE_NAME': table_name,
    }
    
    if include_dates:
        base_args.update({
            'init_date': init_date,
            'final_date': final_date
        })
    
    return base_args



BIG_CLUSTER_CONFIG = {
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
      "spark:spark.dynamicAllocation.minExecutors": "5",    
      "spark:spark.dynamicAllocation.maxExecutors": "48",   
      "spark:spark.dynamicAllocation.initialExecutors": "30", 
      "spark:spark.scheduler.mode": "FAIR",                  
      "spark:spark.task.maxFailures": "8",
      "spark:spark.stage.maxConsecutiveAttempts": "4",
      "spark:spark.locality.wait": "10s",                   # Nuevo
      "spark:spark.shuffle.service.enabled": "true" ,         # Nuevo
      "spark:spark.executor.memoryOverhead": "512m",  # Ajustar overhead memory
    }
  },
  "endpoint_config": {
    "enable_http_port_access": True
  }
}


SMALL_CLUSTER_CONFIG = {
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
    "machine_type_uri": "n2-highmem-4",  # 4 vCPU, 32GB RAM
    "disk_config": {
      "boot_disk_type": "pd-ssd", 
      "boot_disk_size_gb": 100
    }
  },
  "worker_config": {
    "num_instances": 4,
    "machine_type_uri": "n2-highmem-4",  # 4 vCPU, 32GB RAM per worker
    "disk_config": {
      "boot_disk_type": "pd-ssd", 
      "boot_disk_size_gb": 200
    }
  },
  "secondary_worker_config": {
    "num_instances": 0,  # Removed preemptible workers for stability
    "is_preemptible": False,
  },
  "software_config": {
    "image_version":"2.1.85-debian11",
    "properties": {
      # Core Spark configuration
      "spark:spark.scheduler.mode": "FAIR",
      "spark:spark.executor.memory": "6g",
      "spark:spark.driver.memory": "6g",
      "spark:spark.executor.cores": "2",
      "spark:spark.executor.memoryOverhead": "2g",
      
      # Dynamic allocation
      "spark:spark.dynamicAllocation.enabled": "true",
      "spark:spark.dynamicAllocation.minExecutors": "2",
      "spark:spark.dynamicAllocation.maxExecutors": "8",
      "spark:spark.dynamicAllocation.initialExecutors": "4",
      
      # YARN configuration - Total cluster memory: 4 workers * 32GB = 128GB
      # Reserve ~4GB per node for system, so ~28GB available per worker
      "yarn:yarn.nodemanager.resource.memory-mb": "28672",  # 28GB per worker
      "yarn:yarn.scheduler.maximum-allocation-mb": "28672",
      "capacity-scheduler:yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
      
      # Spark SQL optimizations for Oracle JDBC
      "spark:spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
      "spark:spark.sql.sources.parallelPartitionDiscovery.threshold": "32",
      "spark:spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",  # 128MB
      "spark:spark.sql.sources.partitionOverwriteMode": "dynamic",
      "spark:spark.hadoop.parquet.memory.pool.ratio": "0.3",
      
      # Additional performance optimizations
      "spark:spark.yarn.am.memory": "2g",
      "spark:spark.task.maxFailures": "8",
      "spark:spark.stage.maxConsecutiveAttempts": "4",
      "spark:spark.locality.wait": "10s",
      "spark:spark.shuffle.service.enabled": "true",
      "dataproc:dataproc.conscrypt.provider.enable": "false",
      
      # Adaptive query execution
      "spark:spark.sql.adaptive.enabled": "true",
      "spark:spark.sql.adaptive.coalescePartitions.enabled": "true",
    }
  },
  "endpoint_config": {
    "enable_http_port_access": True
  }
}

default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 4,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG(
  'saul_verificaciones_data_pipeline',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
)

landing = BashOperator(task_id='landing',bash_command='echo init landing',dag=dag)


@task_group(group_id='init_landing',dag=dag)
def init_landing():
  
  
  validate_date_interval = BigQueryInsertJobOperator(
    task_id="validate_date_interval",
    configuration={
      "query": {
        "query": "SELECT DATE_DIFF(DATE '{{task.params.init_date}}', DATE '{{task.params.final_date}}', DAY) AS days_diff;",
        "useLegacySql": False,
      },
    },
    params={
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  create_big_cluster = DataprocCreateClusterOperator(
    task_id="create_big_cluster",
    project_id="qlts-nonprod-data-tools",
    cluster_config=BIG_CLUSTER_CONFIG,
    region="us-central1",
    cluster_name="verificaciones-dataproc",
    num_retries_if_resource_is_not_ready=3,
    dag=dag
  )
  
  create_small_cluster = DataprocCreateClusterOperator(
    task_id="create_small_cluster",
    project_id="qlts-nonprod-data-tools",
    cluster_config=SMALL_CLUSTER_CONFIG,
    region="us-central1",
    cluster_name="verificaciones-dataproc",
    num_retries_if_resource_is_not_ready=3,
    dag=dag
  )
  
  select_cluster_creator = BranchPythonOperator(
    task_id="select_cluster_creator",
    python_callable=get_cluster_tipe_creator,
    op_kwargs={
      'init_date':init_date,
      'final_date':final_date,
      'small_cluster_label': 'init_landing.create_small_cluster',
      'big_cluster_label': 'init_landing.create_big_cluster'
    },
    provide_context=True,
    dag=dag
  )  
  

  
  get_datafusion_instance = CloudDataFusionGetInstanceOperator(
    task_id="get_datafusion_instance",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    trigger_rule='one_success',
    project_id='qlts-nonprod-data-tools',
    dag=dag,
  )
  
  validate_date_interval>>select_cluster_creator>>[create_big_cluster,create_small_cluster] >> get_datafusion_instance
  
 
@task_group(group_id='landing_bsc_siniestros_1',dag=dag)
def landing_bsc_siniestros_1():
  
  load_apercab_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_apercab_bsc",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='qlts_dev_verificaciones_apercab_bsc_bkp',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    pipeline_timeout=3600,
    asynchronous=False,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_runtime_args('APERCAB_BSC'),
    dag=dag
  )

  load_maseg_bsc = CloudDataFusionStartPipelineOperator(
    task_id="load_maseg_bsc",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='qlts_dev_verificaciones_maseg_bkp',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args=get_datafusion_runtime_args('MASEG_BSC', include_dates=False),
    dag=dag
  )

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
    runtime_args=get_datafusion_runtime_args('PAGOPROVE'),
    dag=dag
  )
  
@task_group(group_id='landing_bsc_siniestros_2',dag=dag)
def landing_bsc_siniestros_2():

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
    runtime_args=get_datafusion_runtime_args('PAGOSPROVEEDORES'),
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
    runtime_args=get_datafusion_runtime_args('PRESTADORES', include_dates=False),
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
    runtime_args=get_datafusion_runtime_args('RESERVAS_BSC'),
    dag=dag
  )


@task_group(group_id='landing_bsc_siniestros_3',dag=dag)
def landing_bsc_siniestros_3():

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
    runtime_args=get_datafusion_runtime_args('TESTADO_BSC', include_dates=False),
    dag=dag
  )

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
    runtime_args=get_datafusion_runtime_args('TIPOPROVEEDOR', include_dates=False),
    dag=dag
  )

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
    runtime_args=get_datafusion_runtime_args('TSUC_BSC'),
    dag=dag
  )

  
@task_group(group_id='end_injection',dag=dag)
def end_injection():
  
  delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id="qlts-nonprod-data-tools",
    cluster_name="verificaciones-dataproc",
    region="us-central1",
  )
  

# Define pipeline execution order
init = init_landing()

# Ejecuta todos los landing en paralelo
landing_1 = landing_bsc_siniestros_1()
landing_2 = landing_bsc_siniestros_2()
landing_3 = landing_bsc_siniestros_3()

# Fin de la inyección
end = end_injection()

# Dependencias
landing >> init
init >> [landing_1, landing_2, landing_3] >> end