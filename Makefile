include .env

composer-update-dags:
	gsutil cp -r ./dags/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/dags/

composer-update-workspaces:
	gsutil cp -r ./workspaces/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/
	
composer-update-all: composer-update-dags composer-update-workspaces

qlts-composer-update-all:
	gsutil cp ./dags/verificaciones_data_pipeline.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_data_pipeline.py;\
	gsutil cp ./dags/verificaciones_data_pipeline_dataproc_custom.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_data_pipeline_dataproc_custom.py;\
	gsutil cp ./dags/verificaciones_data_pipeline_dataproc.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_data_pipeline_custom_parallel.py;\
	gsutil -m cp -r ./dags/lib/* gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/lib/;\
	gsutil -m cp -r ./workspaces/* gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/;

