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

from airflow.providers.google.cloud.operators.dataproc import  ClusterGenerator

from lib.utils import get_bucket_file_contents

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from airflow.utils.trigger_rule import TriggerRule

# --- Configs ---
PROJECT_ID = 'qlts-nonprod-data-tools'
REGION = 'us-central1'
CLUSTER_NAME = 'create-dataproc-cluster'
INSTANCE_NAME = 'qlts-data-fusion-dev'
PIPELINE_NAME_1 = 'saul_qlts_dev_verificaciones_apercab_bsc'

init_date = '2025-03-01'
final_date = '2025-03-31'

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


##########
CLUSTER_CONFIG = {
    "gce_cluster_config": {
    "internal_ip_only": True,
    "subnetwork_uri": "projects/shared-nonprod-eba6/regions/us-central1/subnetworks/qlts-svpc-non-prd-sn",
    "service_account": "dataproc-dev-operaciones@qlts-nonprod-data-tools.iam.gserviceaccount.com",
    "shielded_instance_config": {
        "enable_secure_boot": False,
        "enable_vtpm": False,
        "enable_integrity_monitoring": False,
        },
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-custom-2-8192",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-custom-2-8192",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},

    },
    "software_config": {
        "image_version": "2.1.85-debian11",
         "properties": {
                    "dataproc:dataproc.conscrypt.provider.enable": "false",
                    "capacity-scheduler:yarn.scheduler.capacity.resource-calculator" : "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator"
                }
    },
    "endpoint_config": {
        "enable_http_port_access": True
    }


}


# CLUSTER_GENERATOR = ClusterGenerator(
#     project_id=PROJECT_ID,
#     cluster_name=CLUSTER_NAME,
#     region=REGION,
#     zone='us-central1-a',
#     master_machine_type='e2-custom-2-8192',
#     master_disk_type='pd-standard',
#     master_disk_size=32,
#     num_masters=1,
#     worker_machine_type='e2-custom-2-8192',
#     worker_disk_type='pd-standard',
#     worker_disk_size=32,
#     num_workers=2,
#     image_version='2.1.85-debian11',
#     subnetwork_uri="projects/shared-nonprod-eba6/regions/us-central1/subnetworks/qlts-svpc-non-prd-sn",
#     service_account="dataproc-dev-operaciones@qlts-nonprod-data-tools.iam.gserviceaccount.com",
#     internal_ip_only=True,
#     enable_component_gateway=True,
#     properties={
#         "dataproc:dataproc.conscrypt.provider.enable": "false",
#         "capacity-scheduler:yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator"
#     }
# ).make()  # T





with DAG(
    dag_id='dag_create_cluster',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['CREATE', 'dataproc', 'test'],
) as dag:
    
    init_creation = BashOperator(task_id='init_creation',bash_command=' echo Creating cluster...',dag=dag)

    
    # # Task 1: Create the cluster (idempotent if the cluster already exists)
    # create_cluster = DataprocCreateClusterOperator(
    #     task_id='create_dataproc_cluster',
    #     project_id=PROJECT_ID,
    #     cluster_name=CLUSTER_NAME,
    #     region=REGION,
    #     cluster_config=CLUSTER_CONFIG,
    #     timeout =  600,
    #     polling_interval_seconds=60,
    #     deferrable= True,
    #     use_if_exists = True
    # )

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
        timeout=600,
        polling_interval_seconds=60,
        deferrable=True,
        use_if_exists=True
    )

    finish_creation = BashOperator(task_id='finish_creation',bash_command='echo Cluster succesfully created.',dag=dag)



    # DAG dependency chain
    init_creation  >> create_cluster >> finish_creation
