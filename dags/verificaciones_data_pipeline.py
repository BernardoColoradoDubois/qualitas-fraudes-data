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
    "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "worker_config": {
    "num_instances": 8,
     "machine_type_uri": "e2-custom-2-8192",
    "disk_config": {
      "boot_disk_type": "pd-standard", "boot_disk_size_gb": 32
    }
  },
  "secondary_worker_config": {
    "num_instances": 1,
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
      "spark:spark.executor.memory": "1.5g",                   # Ajustar memoria
      "spark:spark.driver.memory": "2g",                     # Mantener
      "spark:spark.executor.instances": "4",                 # Aumentar
      "spark:spark.yarn.am.memory": "1g",                   
      "spark:spark.dynamicAllocation.enabled": "true",      
      "spark:spark.dynamicAllocation.minExecutors": "2",    
      "spark:spark.dynamicAllocation.maxExecutors": "16",   
      "spark:spark.dynamicAllocation.initialExecutors": "8", 
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


default_args = {
  'start_date': airflow.utils.dates.days_ago(0),
  'retries': 4,
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
    pipeline_name='qlts_dev_verificaciones_apercab_bsc',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    pipeline_timeout=3600,
    asynchronous=False,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
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
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",    
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'PAGOSPROVEEDORES',
      'init_date':init_date, 
      'final_date':final_date,
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
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
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
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

@task_group(group_id='landing_siniestros_1',dag=dag)
def landing_siniestros_1():

  load_analistas = CloudDataFusionStartPipelineOperator(
    task_id="load_analistas",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_analistas',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'ANALISTAS',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

  load_cat_causa = CloudDataFusionStartPipelineOperator(
    task_id="load_cat_causa",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_cat_causa',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'CAT_CAUSA'
    },
    dag=dag
  )

  load_cobranza = CloudDataFusionStartPipelineOperator(
    task_id="load_cobranza",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_cobranza',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'COBRANZA',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )


@task_group(group_id='landing_siniestros_2',dag=dag)
def landing_siniestros_2():

  load_cobranza_hist = CloudDataFusionStartPipelineOperator(
    task_id="load_cobranza_hist",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_cobranza_hist',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      "system.spark.spark.default.parallelism":"6",
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'4096',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'COBRANZA_HIST',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

  load_etiqueta_siniestro = CloudDataFusionStartPipelineOperator(
    task_id="load_etiqueta_siniestro",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_etiqueta_siniestro',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'ETIQUETA_SINIESTRO',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

  load_registro = CloudDataFusionStartPipelineOperator(
    task_id="load_registro",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_registro',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'REGISTRO',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

  load_sas_sinies = CloudDataFusionStartPipelineOperator(
    task_id="load_sas_sinies",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_sas_sinies',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'SAS_SINIES',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

  load_sas_sinies >> load_etiqueta_siniestro
  load_cobranza_hist >> load_registro

@task_group(group_id='landing_sise',dag=dag)
def landing_sise():

  load_fraud_di = CloudDataFusionStartPipelineOperator(
    task_id="load_fraud_di",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_fraud_di',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'FRAUD_DI',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

  load_fraud_pv = CloudDataFusionStartPipelineOperator(
    task_id="load_fraud_pv",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_fraud_pv',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'FRAUD_PV',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

  load_fraud_rp = CloudDataFusionStartPipelineOperator(
    task_id="load_fraud_rp",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_fraud_rp',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'FRAUD_RP',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )

@task_group(group_id='landing_dua',dag=dag)
def landing_dua():
  
  load_datos_dua = CloudDataFusionStartPipelineOperator(
    task_id="load_datos_dua",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='load_datos_dua',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'LAN_VERIFICACIONES',
      'TABLE_NAME':'DATOS_DUA',
      'init_date':init_date, 
      'final_date':final_date
    },
    dag=dag
  )
  
elt = BashOperator(task_id='elt',bash_command='echo init landing',dag=dag)

@task_group(group_id='recreate_cluster',dag=dag)
def recreate_cluster():
  
  delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id="qlts-nonprod-data-tools",
    cluster_name="verificaciones-dataproc",
    region="us-central1",
  )
  
  select_cluster_creator = BranchPythonOperator(
    task_id="select_cluster_creator",
    python_callable=get_cluster_tipe_creator,
    op_kwargs={
      'init_date':init_date,
      'final_date':final_date,
      'small_cluster_label': 'recreate_cluster.create_small_cluster',
      'big_cluster_label': 'recreate_cluster.create_big_cluster'
    },
    provide_context=True,
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
  
  get_datafusion_instance = CloudDataFusionGetInstanceOperator(
    task_id="get_datafusion_instance",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    trigger_rule='one_success',
    project_id='qlts-nonprod-data-tools',
    dag=dag,
  )
  
  delete_cluster >> select_cluster_creator >> [create_big_cluster,create_small_cluster] >> get_datafusion_instance
  
  
  
@task_group(group_id='bq_elt',dag=dag)
def bq_elt():

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
      'SOURCE_SECOND_TABLE_NAME': 'ESTADOS_MEXICO',
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

  stg_registro = BigQueryInsertJobOperator(
    task_id="stg_registro",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/REGISTRO/STG_REGISTRO.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'REGISTRO',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_REGISTRO',
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
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_REGISTRO',
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

  stg_dua = BigQueryInsertJobOperator(
    task_id="stg_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/DUA/STG_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'DATOS_DUA',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_DUA',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  rtl_dua = BigQueryInsertJobOperator(
    task_id="rtl_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/DUA/RTL_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_DUA',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
      'DEST_TABLE_NAME': 'RTL_DUA',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_dua = BigQueryInsertJobOperator(
    task_id="dm_dua",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/DUA/DM_DUA.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'RTL_DUA',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
      'DEST_TABLE_NAME': 'DM_DUA',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_polizas_vigentes_1 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'FRAUD_PV',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_1',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_polizas_vigentes_2 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_1',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_2',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_polizas_vigentes_3 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_2',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_3',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_polizas_vigentes_4 = BigQueryInsertJobOperator(
    task_id="stg_polizas_vigentes_4",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/POLIZAS_VIGENTES/STG_POLIZAS_VIGENTES_4.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_3',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_POLIZAS_VIGENTES_4',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  rtl_polizas_vigentes = BigQueryInsertJobOperator(
    task_id="rtl_polizas_vigentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/POLIZAS_VIGENTES/RTL_POLIZAS_VIGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_POLIZAS_VIGENTES_4',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
      'DEST_TABLE_NAME': 'RTL_POLIZAS_VIGENTES',
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_polizas_vigentes = BigQueryInsertJobOperator(
    task_id="dm_polizas_vigentes",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/POLIZAS_VIGENTES/DM_POLIZAS_VIGENTES.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'RTL_POLIZAS_VIGENTES',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
      'DEST_TABLE_NAME': 'DM_POLIZAS_VIGENTES',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )  

  stg_pagos_polizas = BigQueryInsertJobOperator(
    task_id="stg_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/PAGOS_POLIZAS/STG_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'FRAUD_RP',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_PAGOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  rtl_pagos_polizas = BigQueryInsertJobOperator(
    task_id="rtl_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/PAGOS_POLIZAS/RTL_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_PAGOS_POLIZAS',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
      'DEST_TABLE_NAME': 'RTL_PAGOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_pagos_polizas = BigQueryInsertJobOperator(
    task_id="dm_pagos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/PAGOS_POLIZAS/DM_PAGOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'RTL_PAGOS_POLIZAS',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
      'DEST_TABLE_NAME': 'DM_PAGOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
  
  stg_incisos_polizas_1 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_1",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_1.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'LAN_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'FRAUD_DI',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_1',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_incisos_polizas_2 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_2",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_2.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_1',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_2',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_incisos_polizas_3 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_3",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_3.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_2',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_3',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  stg_incisos_polizas_4 = BigQueryInsertJobOperator(
    task_id="stg_incisos_polizas_4",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/INCISOS_POLIZAS/STG_INCISOS_POLIZAS_4.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_3',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'STG_VERIFICACIONES',
      'DEST_TABLE_NAME': 'STG_INCISOS_POLIZAS_4',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  rtl_incisos_polizas = BigQueryInsertJobOperator(
    task_id="rtl_incisos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/INCISOS_POLIZAS/RTL_INCISOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'STG_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'STG_INCISOS_POLIZAS_4',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'RTL_VERIFICACIONES',
      'DEST_TABLE_NAME': 'RTL_INCISOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )

  dm_incisos_polizas = BigQueryInsertJobOperator(
    task_id="dm_incisos_polizas",
    configuration={
      "query": {
        "query": get_bucket_file_contents(path='gs://us-central1-qlts-composer-d-cc034e9e-bucket/workspaces/models/INCISOS_POLIZAS/DM_INCISOS_POLIZAS.sql'),
        "useLegacySql": False,
      }
    },
    params={
      'SOURCE_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'SOURCE_DATASET_NAME': 'RTL_VERIFICACIONES',
      'SOURCE_TABLE_NAME': 'RTL_INCISOS_POLIZAS',
      'DEST_PROJECT_ID': 'qlts-dev-mx-au-bro-verificacio',
      'DEST_DATASET_NAME': 'DM_VERIFICACIONES',
      'DEST_TABLE_NAME': 'DM_INCISOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    location='us-central1',
    gcp_conn_id="google_cloud_default",
    dag=dag 
  )
 
  rtl_pagos_proveedores  >> dm_pagos_proveedores
  rtl_coberturas_movimientos >> dm_coberturas_movimientos
  stg_etiqueta_siniestro_1 >> stg_etiqueta_siniestro_2 >> stg_etiqueta_siniestro_3 >> rtl_etiqueta_siniestro >> dm_etiqueta_siniestro
  stg_registro >> rtl_registro >> dm_registro
  stg_siniestros >> rtl_siniestros >> dm_siniestros
  stg_dua >> rtl_dua >> dm_dua
  stg_polizas_vigentes_1 >> stg_polizas_vigentes_2 >> stg_polizas_vigentes_3 >> stg_polizas_vigentes_4 >> rtl_polizas_vigentes >> dm_polizas_vigentes
  stg_pagos_polizas >> rtl_pagos_polizas >> dm_pagos_polizas
  stg_incisos_polizas_1 >> stg_incisos_polizas_2 >> stg_incisos_polizas_3 >> stg_incisos_polizas_4 >> rtl_incisos_polizas >> dm_incisos_polizas
 
 
inject = BashOperator(task_id='inject',bash_command='echo init landing',dag=dag)

@task_group(group_id='injection_1',dag=dag)
def injection_1():
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_ASEGURADOS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_ASEGURADOS',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_ASEGURADOS'
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",   
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',   
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_COBERTURAS_MOVIMIENTOS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_COBERTURAS_MOVIMIENTOS',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_COBERTURAS_MOVIMIENTOS',
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",   
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',   
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_ESTADOS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_ESTADOS',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_ESTADOS',
    },
    dag=dag
  )
  
@task_group(group_id='injection_2',dag=dag)
def injection_2():

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
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",   
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',   
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_PAGOS_PROVEEDORES',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_PAGOS_PROVEEDORES',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_PAGOS_PROVEEDORES',
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",   
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',   
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_PROVEEDORES',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_PROVEEDORES',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_PROVEEDORES'
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',    
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_TIPOS_PROVEEDORES',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_TIPOS_PROVEEDORES',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_TIPOS_PROVEEDORES'
    },
    dag=dag
  )
  
@task_group(group_id='injection_3',dag=dag)
def injection_3():

  inject_causas = CloudDataFusionStartPipelineOperator(
    task_id="inject_causas",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inyect_dm_causas',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      "system.spark.spark.default.parallelism":"4",
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',      
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_CAUSAS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_CAUSAS',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_CAUSAS',
      "system.spark.log.level": "DEBUG"

    },
    dag=dag
  )

  inject_etiqueta_siniestro = CloudDataFusionStartPipelineOperator(
    task_id="inject_etiqueta_siniestro",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inyect_dm_etiqueta_siniestro',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",    
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_ETIQUETA_SINIESTRO',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_ETIQUETA_SINIESTRO',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_ETIQUETA_SINIESTRO',
      'init_date':init_date,
      'final_date':final_date
    },
    dag=dag
  )

  inject_registro = CloudDataFusionStartPipelineOperator(
    task_id="inject_registro",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inject_registro',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      "system.spark.spark.default.parallelism":"4",
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',      
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_REGISTRO',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_REGISTRO',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_REGISTRO',
      'init_date':init_date,
      'final_date':final_date
    },
    dag=dag
  )

@task_group(group_id='injection_4',dag=dag)
def injection_4():

  inject_dua = CloudDataFusionStartPipelineOperator(
    task_id="inject_dua",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inject_dm_dua',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      "system.spark.spark.default.parallelism":"8",
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc", 
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',     
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_DUA',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_DUA',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_DUA',
      'init_date':init_date,
      'final_date':final_date
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
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",    
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_OFICINAS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_OFICINAS',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'STG_OFICINAS'
    },
    dag=dag
  )

@task_group(group_id='injection_5',dag=dag)
def injection_5():
  
  inject_polizas_vigentes = CloudDataFusionStartPipelineOperator(
    task_id="inject_polizas_vigentes",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inject_dm_polizas_vigentes',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",  
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',    
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_POLIZAS_VIGENTES',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_POLIZAS_VIGENTES',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_POLIZAS_VIGENTES',
      'init_date':init_date,
      'final_date':final_date
    },
    dag=dag
  )
  
  inject_pagos_polizas = CloudDataFusionStartPipelineOperator(
    task_id="inject_pagos_polizas",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inject_dm_pagos_polizas',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",   
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',   
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_PAGOS_POLIZAS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_PAGOS_POLIZAS',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_PAGOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    dag=dag
  )
  
  inject_incisos_polizas = CloudDataFusionStartPipelineOperator(
    task_id="inject_incisos_polizas",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inject_dm_incisos_polizas',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'1',
      'task.executor.system.resources.memory':'2048',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc",    
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',  
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_INCISOS_POLIZAS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_INCISOS_POLIZAS',
      'init_date':init_date,
      'final_date':final_date
    },
    dag=dag
  )
  
  
@task_group(group_id='injection_6',dag=dag)
def injection_6():
  
  inject_siniestros = CloudDataFusionStartPipelineOperator(
    task_id="inject_siniestros",
    location='us-central1',
    instance_name='qlts-data-fusion-dev',
    namespace='verificaciones',
    pipeline_name='inyect_dm_siniestros',
    project_id='qlts-nonprod-data-tools',
    pipeline_type = DataFusionPipelineType.BATCH,
    success_states=["COMPLETED"],
    asynchronous=False,
    pipeline_timeout=3600,
    deferrable=True,
    poll_interval=30,
    runtime_args={
      'app.pipeline.overwriteConfig':'true',
      'task.executor.system.resources.cores':'2',
      'task.executor.system.resources.memory':'3072',
      'dataproc.cluster.name':'verificaciones-dataproc',
      "system.profile.name" : "USER:verificaciones-dataproc", 
      'APP_ORACLE_DRIVER_NAME':'Oracle 8',
      'APP_ORACLE_HOST':'qualitas-clm.cgriqmyweq5c.us-east-2.rds.amazonaws.com',
      'APP_ORACLE_PORT':'1521',
      'APP_ORACLE_SERVICE_NAME':'ORCL',
      'APP_ORACLE_USER':'ADMIN',
      'APP_ORACLE_PASSWORD':'FqzJ3n3Kvwcftakshcmi',     
      'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
      'DATASET_NAME':'DM_VERIFICACIONES',
      'TABLE_NAME':'DM_SINIESTROS',
      'INJECT_SCHEMA_NAME':'RAW_INSUMOS',
      'INJECT_TABLE_NAME':'STG_SINIESTROS',
      'INSUMOS_SCHEMA_NAME':'INSUMOS',
      'INSUMOS_TABLE_NAME':'DM_SINIESTROS',
      'init_date':init_date,
      'final_date':final_date
    },
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
  
end = BashOperator(task_id='end',bash_command='echo end landing',dag=dag)

landing >> init_landing() >> landing_bsc_siniestros_1() >> landing_bsc_siniestros_2() >> landing_bsc_siniestros_3() >> landing_siniestros_1() >> landing_siniestros_2() >> landing_sise() >> landing_dua() >> elt
elt >> bq_elt() >> inject
elt >> recreate_cluster() >> inject
inject  >> injection_1() >> injection_2() >> injection_3() >> injection_4()>> injection_5() >> injection_6() >> end_injection() >> end