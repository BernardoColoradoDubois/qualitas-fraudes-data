include .env

composer-update-dags:
	gsutil cp ./dags/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/dags/

composer-update-workspaces:
	gsutil cp ./workspaces/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/

composer-update-all: composer-update-dags composer-update-workspaces