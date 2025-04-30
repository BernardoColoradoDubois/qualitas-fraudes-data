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

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

from airflow.utils.trigger_rule import TriggerRule

# --- Configs ---
PROJECT_ID = 'qlts-nonprod-data-tools'
REGION = 'us-central1'
CLUSTER_NAME = 'shared-dataproc-cluster'
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
        "num_instances": 8,
        "machine_type_uri": "e2-custom-2-8192",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},

    },
    "software_config": {
        "image_version": "2.1.85-debian11",
         "properties": {
                    "dataproc:dataproc.conscrypt.provider.enable": "false",
                    "capacity-scheduler:yarn.scheduler.capacity.resource-calculator" : "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator"
                }
    }



}


with DAG(
    dag_id='saul_cluster_then_run_pipelines',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['datafusion', 'dataproc', 'test'],
) as dag:
    
    get_datafusion_instance = CloudDataFusionGetInstanceOperator(
        task_id="get_datafusion_instance",
        location=REGION,
        instance_name=INSTANCE_NAME,
        project_id=PROJECT_ID,
        dag=dag,
      )


    # Task 1: Create the cluster (idempotent if the cluster already exists)
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
    )

    # Task 2: Run first pipeline using this cluster
    run_pipeline_1 = CloudDataFusionStartPipelineOperator(
        task_id='run_pipeline_1',
        location=REGION,
        instance_name=INSTANCE_NAME,
        namespace='verificaciones',
        pipeline_name=PIPELINE_NAME_1,
        project_id=PROJECT_ID,


        pipeline_type = DataFusionPipelineType.BATCH,
        success_states=["COMPLETED"],
        asynchronous=False,
        pipeline_timeout=3600,
        deferrable=True,
        poll_interval=30,
        runtime_args={
            "system.dataproc.cluster.name": CLUSTER_NAME,
            "system.dataproc.region": REGION,
            "system.dataproc.project.id": PROJECT_ID,
            #"system.profile.name" : "USER:verificaciones-dataproc",

            'TEMPORARY_BUCKET_NAME':'gcs-qlts-dev-mx-au-bro-verificaciones',
            'DATASET_NAME':'LAN_VERIFICACIONES',
            'TABLE_NAME':'APERCAB_BSC',
            'init_date':init_date, 
            'final_date':final_date
        },
    )


    # Task 4: Delete the cluster regardless of pipeline success/failure
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE  # ensures it runs even if upstream fails
    )


    # DAG dependency chain
    create_cluster >> get_datafusion_instance >> run_pipeline_1 >> delete_cluster
