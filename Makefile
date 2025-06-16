include .env

composer-update-dags:
	gsutil cp -r ./dags/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/dags/

composer-update-workspaces:
	gsutil cp -r ./workspaces/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/
	
composer-update-all: composer-update-dags composer-update-workspaces

qlts-composer-update-all:
	gsutil cp ./dags/verificaciones_data_pipeline.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_data_pipeline.py;\
	gsutil cp ./dags/verificaciones_data_pipeline_dataproc_custom.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_data_pipeline_dataproc_custom.py;\
	gsutil cp ./dags/verificaciones_data_pipeline_custom_parallel.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_data_pipeline_custom_parallel.py;\
	gsutil cp ./dags/verificaciones_elt.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_elt.py;\
	gsutil cp ./dags/verificaciones_inject.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/verificaciones_inject.py;\
	gsutil cp ./dags/create_cluster.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/create_cluster.py;\
	gsutil cp ./dags/delete_cluster.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/delete_cluster.py;\
	gsutil cp ./dags/create_calendar.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/create_calendar.py;\
	gsutil cp ./dags/range_test.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/range_test.py;\
	gsutil cp ./dags/qualitas_verificaciones_data_pipeline.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_data_pipeline.py;\
	gsutil cp ./dags/qualitas_verificaciones_data_pipeline_rocket.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_data_pipeline_rocket.py;\
	gsutil cp ./dags/qualitas_verificaciones_data_pipeline_dev.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_data_pipeline_dev.py;\
	gsutil -m cp -r ./dags/lib/* gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/lib/;\
	gsutil -m cp -r ./workspaces/* gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/;

