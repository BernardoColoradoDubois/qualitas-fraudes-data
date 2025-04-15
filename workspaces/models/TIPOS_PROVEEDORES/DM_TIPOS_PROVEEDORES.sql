CREATE OR REPLACE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}`  AS
SELECT
id as ID,
tipo_proveedor as TIPO_PROVEEDOR,
Grupo as GRUPO,
nombre_grupo as NOMBRE_GRUPO
FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`