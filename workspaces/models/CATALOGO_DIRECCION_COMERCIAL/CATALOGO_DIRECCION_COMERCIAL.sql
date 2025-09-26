CREATE OR REPLACE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}` AS
SELECT 
  -- NO_OF: Integer -> INT64
  SAFE_CAST(NO_OF AS INT64) AS NO_OF,
  
  -- OFICINA: String -> STRING (mantiene como string)
  OFICINA,
  
  -- ZONA_ATENCION: String -> STRING (mantiene como string)
  ZONA_ATENCION,
  
  -- DIRECTOR_GENERAL: String -> STRING (mantiene como string)
  DIRECTOR_GENERAL,
  
  -- SUBDIRECTOR_GENERAL: String -> STRING (mantiene como string)
  SUBDIRECTOR_GENERAL

FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`;