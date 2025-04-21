DELETE *
FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`
WHERE CAST(FECHA_CARGA AS DATE) BETWEEN `{{task.params.init_date}}` AND `{{task.params.final_date}}`;
