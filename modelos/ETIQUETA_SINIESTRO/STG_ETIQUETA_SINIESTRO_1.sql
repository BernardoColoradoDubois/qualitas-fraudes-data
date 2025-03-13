-- Crea la tabla STG_ETIQUETA_SINIESTRO_1 en el esquema STG_FRAUDES 
--y crea una columna Z_ID con un valor por defecto si es nulo y una columna REPORTE con un valor por defecto si es nulo
CREATE OR REPLACE TABLE `STG_FRAUDES.STG_ETIQUETA_SINIESTRO_1` AS 
SELECT
  CASE  
    WHEN Z_ID IS NULL THEN CONCAT(REPORTE,'00')
    ELSE Z_ID
  END AS Z_ID
  ,CASE 
    WHEN REPORTE IS NULL THEN SUBSTRING(Z_ID, 1, (LENGTH(Z_ID)-2))
    ELSE REPORTE
  END AS REPORTE
  ,SINIESTRO
  ,COD_STATUS
  ,FEC_INV
  ,TEXT_INVEST
  ,USUARIO_INV
  ,TEXT_LOC
  ,FEC_LOC
  ,FEC_CARGA
  ,HORA_PROCESO
  ,BATCHDATE
  ,SYSUSERID
FROM `sample_landing_siniestros.etiqueta_siniestro`
WHERE SINIESTRO <> '0424'
