import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import timedelta

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator

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
      "spark:spark.dynamicAllocation.minExecutors": "5",    
      "spark:spark.dynamicAllocation.maxExecutors": "48",   
      "spark:spark.dynamicAllocation.initialExecutors": "30", 
      "spark:spark.scheduler.mode": "FIFO",                  
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
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  'create_cluster',
  default_args=default_args,
  description='liveness monitoring dag',
  schedule_interval='0 0 1 1 *',
  max_active_runs=2,
  catchup=False,
  dagrun_timeout=timedelta(minutes=120),
  tags=['MX','AUTOS','VERIFICACIONES','INSUMOS']
)

create_cluster = DataprocCreateClusterOperator(
  task_id="create_cluster",
  project_id="qlts-nonprod-data-tools",
  cluster_config=CLUSTER_CONFIG,
  region="us-central1",
  cluster_name="verificaciones-dataproc",
  num_retries_if_resource_is_not_ready=3,
  dag=dag
)