CREATE SCHEMA IF NOT EXISTS `{{task.params.PROJECT_ID}}.{{task.params.DATASET_NAME}}`
OPTIONS (
  location = '{{task.params.LOCATION}}',
  description = 'Este es el nuevo dataset para capa bronce.'
);