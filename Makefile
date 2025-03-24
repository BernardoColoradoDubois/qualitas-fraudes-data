include .env

composer-update-dags:
	gsutil cp -r ./dags/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/dags/

composer-update-workspaces:
	gsutil cp -r ./workspaces/* gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/workspaces/


composer-update-environment:
	gsutil cp ./requirements.txt gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/requirements.txt;\
	gsutil cp ./startup.sh gs://${GCP_COMPOSER_WORK_BUCKET_NAME}/startup.sh;\


composer-update-all: composer-update-dags composer-update-workspaces composer-update-environment
