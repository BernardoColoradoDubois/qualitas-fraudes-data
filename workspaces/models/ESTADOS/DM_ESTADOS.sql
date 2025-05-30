TRUNCATE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}`;
INSERT INTO `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}`
SELECT
  Z_ID AS ID,
  EDOPOB AS ESTADO_POBLACION,
  ESTATUS,
  CVEMPIO_INEGI AS CLAVE_INEGI,
  REGION,
  RADIOPERFIL AS RADIO_PERFIL,
  EDO_ESTANDAR AS ESTADO_ESTANDAR,
  EDOPOB2 AS ESTADO_POBLACION_2,
  E.CODIGO_ESTADO,
  E.ESTADO
FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}` L 
INNER JOIN `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_SECOND_TABLE_NAME}}` E ON SUBSTRING(L.Z_ID,0,2) = E.CODIGO_ESTADO;