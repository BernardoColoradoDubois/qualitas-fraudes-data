include .env

composer-update-dags:
	gsutil cp -r ./dags/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/dags/

composer-update-workspaces:
	gsutil cp -r ./workspaces/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/
	
composer-update-all: composer-update-dags composer-update-workspaces

qlts-composer-update-all:
	gsutil cp ./dags/create_cluster.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/create_cluster.py;\
	gsutil cp ./dags/delete_cluster.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/delete_cluster.py;\
	gsutil cp ./dags/create_calendar.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/create_calendar.py;\
	gsutil cp ./dags/range_test.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/range_test.py;\
	gsutil cp ./dags/qualitas_verificaciones_data_pipeline.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_data_pipeline.py;\
	gsutil cp ./dags/qualitas_verificaciones_data_pipeline_rocket.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_data_pipeline_rocket.py;\
	gsutil cp ./dags/qualitas_verificaciones_data_pipeline_dev.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_data_pipeline_dev.py;\
	gsutil cp ./dags/qualitas_verificaciones_data_pipeline_qa.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_data_pipeline_qa.py;\
	gsutil cp ./dags/qualitas_prevencion_fraudes_data_pipeline.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_prevencion_fraudes_data_pipeline.py;\
	gsutil cp ./dags/qualitas_verificaciones_single_load_data_pipeline.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/qualitas_verificaciones_single_load_data_pipeline.py;\
	gsutil cp ./dags/list_python_packages.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/list_python_packages.py;\
	gsutil cp ./dags/load_csv.py gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/load_csv.py;\
	gsutil -m cp -r ./dags/lib/* gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/dags/lib/;\
	gsutil -m cp -r ./workspaces/* gs://${QLTS_GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/;


qlts-upload-csvs:
	gsutil cp ./files/CONTROL_DE_AGENTES_202506_01.csv gs://${QLTS_GCP_VERIFICACIONES_LANDING_BUCKET_NAME}/CONTROL_DE_AGENTES/CONTROL_DE_AGENTES_202506_01.csv;\
	gsutil cp ./files/APERTURA_REPORTE.csv gs://${QLTS_GCP_VERIFICACIONES_LANDING_BUCKET_NAME}/APERTURA_REPORTE/APERTURA_REPORTE.csv;\
	gsutil cp ./files/PRODUCCION1.csv gs://${QLTS_GCP_VERIFICACIONES_LANDING_BUCKET_NAME}/PRODUCCION1/PRODUCCION1.csv;\
	gsutil cp ./files/PRODUCCION2.csv gs://${QLTS_GCP_VERIFICACIONES_LANDING_BUCKET_NAME}/PRODUCCION2/PRODUCCION2.csv;\
	gsutil cp ./files/RECUPERACIONES.csv gs://${QLTS_GCP_VERIFICACIONES_LANDING_BUCKET_NAME}/RECUPERACIONES/RECUPERACIONES.csv;\
	gsutil cp ./files/SUMAS_ASEG_202506_01.csv gs://${QLTS_GCP_VERIFICACIONES_LANDING_BUCKET_NAME}/SUMAS_ASEG/SUMAS_ASEG_202506_01.csv;
	
