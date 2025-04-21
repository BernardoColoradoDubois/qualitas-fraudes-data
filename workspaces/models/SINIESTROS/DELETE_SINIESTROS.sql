DELETE *
FROM `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}`
WHERE CAST(FECHA_REGISTRO AS DATE) BETWEEN `{{task.params.init_date}}` AND `{{task.params.final_date}}`;