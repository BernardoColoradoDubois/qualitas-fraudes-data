CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.EJECUTIVAS_SEG` AS 
  SELECT DISTINCT t1.CUENTA, 
  t1.CVE_AGENTE, 
  t1.EJECUTIVA
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.CLAVES_CTAS_ESPECIALES` t1;
