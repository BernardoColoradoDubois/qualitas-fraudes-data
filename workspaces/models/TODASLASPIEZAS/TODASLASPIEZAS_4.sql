
-- ================================================================================
-- FASE 4: PREPARACIÓN DE INFORMACIÓN BASE
-- ================================================================================


-- Preparación de talleres con columnas adicionales
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.TAB_TALLERES` AS
SELECT 
    t1.IDTALLER,
    t1.IDOFICINA,
    t1.CLAVETALLER,
    t1.NOMBRECOMERCIAL,
    t1.IDENTIDAD,
    t1.TIPOCDR_PORTAL,
    t1.CDRCOTIZADOR,
    t1.CDRAUTOSURTIDO,
    t1.IDESTADO,
    ' ' AS IDENTIDAD_ANTERIOR,
    ' ' AS CERCO_ANTERIOR
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_TALLERES` t1
ORDER BY t1.CLAVETALLER;

-- Unión de estatus del expediente
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ESTATUS_DEL_EXPEDIENTE` AS
SELECT 
    t1.IDEXPEDIENTE,
    t2.DESCRIPCION AS ESTATUSEXPEDIENTE
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_ESTATUS` t1
LEFT JOIN `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_ESTATUSEXPEDIENTES` t2 ON t1.IDESTATUSEXPEDIENTE = t2.IDESTATUSEXPEDIENTE;

-- Datos del vehículo con marca
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DATOS_DEL_VEH_CON_MARCA` AS
SELECT 
    t1.IDEXPEDIENTE,
    t1.TIPO AS TIPOVEHICULO,
    t1.MODELO,
    t1.SERIE,
    t2.DESCRIPCION AS MARCAVEHICULO
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_DATOSVEHICULO` t1
LEFT JOIN `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_MARCA` t2 ON t1.IDMARCA = t2.IDMARCA;

-- Categoría del valuador
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.CATEGORIA_VALUADOR` AS
SELECT 
    t1.CODVALUADOR,
    t1.IDCATEGORIA,
    t1.EQUIPOPESADO,
    t2.NOMBRE AS CATEGORIAVALUADOR,
    CASE WHEN t1.EQUIPOPESADO = 1 THEN "EQUIPO PESADO" ELSE "AUTOS" END AS TIPOVALUADOR
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_VALUADOR` t1
LEFT JOIN `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_CATEGORIA` t2 ON t1.IDCATEGORIA = t2.IDCATEGORIA;

-- Analista CDR
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ANALISTA_CDR` AS
SELECT DISTINCT 
    t3.CLAVESUPERVISOR,
    t3.NOMBRESUPERVISOR AS ANALISTACDR,
    t2.CLAVETALLER,
    t3.IDREGIONGEOGRAFICA
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_DATOSGENERALES` t1
LEFT JOIN `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_RELACIONCDR_SICDR` t2 ON t1.CLAVETALLER = t2.CLAVETALLER
LEFT JOIN `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_SUPERVISORINTEGRAL` t3 ON t2.IDSICDR = t3.IDSUPERVISORINTEGRAL
ORDER BY t3.CLAVESUPERVISOR;
