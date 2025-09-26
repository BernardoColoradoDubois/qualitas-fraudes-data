CREATE OR REPLACE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}` AS
SELECT 
  -- SINIESTRO: Float -> FLOAT64
  SINIESTRO,
  
  -- CAUSA: String -> STRING (mantiene como string)
  CAUSA,
  
  -- DETALLE: String -> STRING (mantiene como string)  
  DETALLE,
  
  -- POLIZA: String
  POLIZA,
  
  -- INCISO: String
  INCISO,
  
  -- ASEGURADO: String -> STRING (mantiene como string)
  ASEGURADO,
  
  -- SERIE: String -> STRING (mantiene como string)
  SERIE,
  
  -- ESTATUS: String -> STRING (mantiene como string)
  ESTATUS,
  
  -- AHORRO: Float -> FLOAT64
  SAFE_CAST(AHORRO AS FLOAT64) AS AHORRO,
  
  -- MES_RECHAZO: Date -> DATE
  SAFE_CAST(MES_RECHAZO AS DATETIME) AS MES_RECHAZO,
  
  -- CVE_AGENTE: String
  CVE_AGENTE,
  
  -- AGENTE: String -> STRING (mantiene como string)
  AGENTE

FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`;