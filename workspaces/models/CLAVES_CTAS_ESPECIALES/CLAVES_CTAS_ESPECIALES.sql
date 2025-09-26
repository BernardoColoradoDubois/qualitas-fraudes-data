CREATE OR REPLACE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}` AS
SELECT 
  -- CUENTA: String -> STRING (mantiene como string)
  CUENTA,
  
  -- CVE_AGENTE: Integer -> INT64
  CVE_AGENTE,
  
  -- EJECUTIVA: String -> STRING (mantiene como string)
  EJECUTIVA

FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`;