CREATE OR REPLACE TABLE `DM_FRAUDES.DM_CAUSAS` AS
  SELECT 
  Z_ID AS ID,
  CAUSA,
  CAUSA_HOMOLOGADA,
  BATCHDATE AS FECHA_LOTE,
  SYSUSERID AS ID_USUARIO_SISTEMA
FROM `sample_landing_siniestros.cat_causa`
WHERE Z_ID IS NOT NULL AND Z_ID <> '*' AND Z_ID <> '9' AND Z_ID <> '2'
ORDER BY Z_ID;