
-- ================================================================================
-- FASE 2: PROCESAMIENTO DE FECHAS DE AUTORIZACIÓN
-- ================================================================================

-- Extracción de ENVIOHISTORICO con fecha mínima
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ENVIOHISTORICO_P` AS
SELECT 
    IDEXPEDIENTE,
    MIN(FECHAAUTORIZACIONVALUADOR) AS MIN_FEC_AUTORIZACION
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ENVIOHISTORICO`
WHERE IDEXPEDIENTE IS NOT NULL
GROUP BY IDEXPEDIENTE
HAVING MIN(FECHAAUTORIZACIONVALUADOR) >= '2019-01-01 00:00:00';

-- Extracción de HISTORICOTERMINOENTREGA con tipofecha autorizada
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.AUTORIZACIONES` AS
SELECT 
    IDEXPEDIENTE,
    TIPOFECHA,
    FECHA
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.HISTORICOTERMINOENTREGA`
WHERE TIPOFECHA LIKE '%AUTORIZA VAL%' OR TIPOFECHA LIKE '%AUTORIZACION VAL%';

-- Fecha mínima de autorización de la tabla AUTORIZACIONES
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.MIN_AUTORIZACION` AS
SELECT 
    IDEXPEDIENTE,
    MIN(FECHA) AS MIN_FEC_AUTORIZACION
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.AUTORIZACIONES`
GROUP BY IDEXPEDIENTE
ORDER BY MIN_FEC_AUTORIZACION;

-- Unión de ENVIOHISTORICO_P y MIN_AUTORIZACION
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.MIN_FEC_AUTORIZACION` AS
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ENVIOHISTORICO_P`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.MIN_AUTORIZACION`;

-- Fecha mínima final de autorización
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.QUERY_FOR_ENVIOHISTORICO` AS
SELECT DISTINCT 
    t1.IDEXPEDIENTE,
    MIN(t1.MIN_FEC_AUTORIZACION) AS MIN_of_FECHAAUTORIZACIONVALUADOR
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.MIN_FEC_AUTORIZACION` t1
WHERE t1.IDEXPEDIENTE IS NOT NULL
GROUP BY t1.IDEXPEDIENTE
HAVING MIN(t1.MIN_FEC_AUTORIZACION) >= '2019-01-01 00:00:00'
ORDER BY MIN_of_FECHAAUTORIZACIONVALUADOR;