DELETE FROM `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}`
WHERE CAST(FECHA_VALUACION AS DATE) BETWEEN '{{task.params.init_date}}' AND '{{task.params.final_date}}';
INSERT INTO `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}` 
SELECT * FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`;