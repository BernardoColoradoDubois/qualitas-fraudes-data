-- ================================================================================
-- TRADUCCIÓN DE SAS SQL A BIGQUERY SQL - TODAS LAS PIEZAS
-- Dataset origen: qlts_bro_op_prevencion_fraudes_dev
-- Dataset staging: STG_PREVENCION_FRAUDES  
-- Dataset final: DM_PREVENCION_FRAUDES
-- ================================================================================

-- ================================================================================
-- FASE 1: EXTRACCIÓN DE CATÁLOGOS
-- ================================================================================

-- qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev
-- qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev

-- Extracción de CAUSACAMBIOVALE
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_CAUSACAMBIOVALE` AS
SELECT 
    IDCAUSACAMBIOVALE,
    DESCRIPCION
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.CAUSACAMBIOVALE`;

-- Extracción de COSTO con el concepto igual a 'REF'
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_COSTO` AS
SELECT 
    CAST(IDCOSTO AS STRING) AS IDCOSTO,
    LISTA,
    BLINDAJE,
    CLAVENAGS,
    MONTOCONVENIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.COSTO`
WHERE CONCEPTO = 'REF';

-- Extracción de COMPLEMENTO con el concepto igual a 'REF'
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_COMPLEMENTO` AS
SELECT 
    IDCOMPLEMENTO,
    LISTA,
    BLINDAJE,
    CLAVENAGS,
    MONTOCONVENIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.COMPLEMENTO`
WHERE CONCEPTO = 'REF';

-- Extracción de VISTA_VALE
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_VISTA_VALE` AS
SELECT 
    CAST(IDEXPEDIENTE AS STRING) AS IDEXPEDIENTE,
    CAST(IDVALE AS STRING) AS IDVALE,
    CODPROVEEDOR,
    FOLIO,
    FECHAEXPEDICION,
    FECHAACTUALIZACION,
    AUTORIZADOR,
    IDVALEESTATUS
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.VISTA_VALE`;

-- Extracción de VALEESTATUS
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_VALEESTATUS` AS
SELECT 
    IDVALEESTATUS,
    DESCRIPCION
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.VALEESTATUS`;

-- Extracción de VALEHISTORICO y creación de LLAVEPIEZA
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_VALEHISTORICO` AS
SELECT 
    IDVALEHISTORICO,
    NUMPARTE,
    REFERENCIA,
    DESCRIPCION,
    MONTO,
    IDCOSTO,
    TIPO,
    FECENVIO,
    FECENTREGAREFACCIONARIA,
    FECRECEPCION,
    FECSURTIDO,
    USUARIO,
    IDREFACCION,
    IDCAUSACAMBIOVALE,
    IDVALE,
    -- LLAVEPIEZA
    CONCAT(IFNULL(NUMPARTE,''), IFNULL(REFERENCIA,''), IFNULL(DESCRIPCION,'')) AS LLAVEPIEZA
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.VALEHISTORICO`;

-- Extracción de RELACIONCDR_SICDR
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_RELACIONCDR_SICDR` AS
SELECT 
    CLAVETALLER,
    IDSICDR
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.RELACIONCDR_SICDR`;

-- Extracción de SUPERVISORINTEGRAL
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_SUPERVISORINTEGRAL` AS
SELECT 
    CLAVESUPERVISOR,
    NOMBRESUPERVISOR,
    IDREGIONGEOGRAFICA,
    IDSUPERVISORINTEGRAL
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.SUPERVISORINTEGRAL`;

-- Extracción de DATOSGENERALES
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_DATOSGENERALES` AS
SELECT 
    CAST(IDEXPEDIENTE AS STRING) AS IDEXPEDIENTE,
    EJERCICIO,
    NUMREPORTE,
    NUMSINIESTRO,
    CLAVETALLER,
    CODVALUADOR,
    IDVALUACION
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_verificaciones_dev.DATOSGENERALES`;

-- Extracción de FECHAS
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_FECHAS` AS
SELECT 
    CAST(IDEXPEDIENTE AS STRING) AS IDEXPEDIENTE,
    FECVALUACION,
    FECTERMINADO,
    FECENTREGADO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.FECHAS`;

-- Extracción de VALUACION
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_VALUACION` AS
SELECT 
    IDVALUACION,
    DESCRIPCION
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.VALUACION`;

-- Extracción de CERCO
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_CERCO` AS
SELECT 
    IDENTIDAD,
    NOMBRE
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.CERCO`
ORDER BY NOMBRE;

-- Extracción de ESTADO
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_ESTADO` AS
SELECT 
    CAST(IDREGIONGEOGRAFICA AS STRING) AS IDREGIONGEOGRAFICA,
    IDESTADO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ESTADO`;

-- Extracción de VALUADOR
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_VALUADOR` AS
SELECT 
    CODVALUADOR,
    IDCATEGORIA,
    EQUIPOPESADO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.VALUADOR`;

-- Extracción de CATEGORIA
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_CATEGORIA` AS
SELECT 
    NOMBRE,
    IDCATEGORIA
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.CATEGORIA`;

-- Extracción de DATOSVEHICULO
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_DATOSVEHICULO` AS
SELECT 
    IDEXPEDIENTE,
    TIPO,
    MODELO,
    SERIE,
    IDMARCA
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_verificaciones_dev.DATOSVEHICULO`;

-- Extracción de MARCA
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_MARCA` AS
SELECT 
    IDMARCA,
    DESCRIPCION
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.MARCA`;

-- Extracción de ESTATUS
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_ESTATUS` AS
SELECT 
    IDEXPEDIENTE,
    IDESTATUSEXPEDIENTE
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ESTATUS`;

-- Extracción de ESTATUSEXPEDIENTES
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_ESTATUSEXPEDIENTES` AS
SELECT 
    IDESTATUSEXPEDIENTE,
    DESCRIPCION
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ESTATUSEXPEDIENTES`;

-- Extracción de TALLERES
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_TALLERES` AS
SELECT 
    IDTALLER,
    IDOFICINA,
    CLAVETALLER,
    NOMBRECOMERCIAL,
    IDENTIDAD,
    TIPO AS TIPOCDR_PORTAL,
    COTIZADOR AS CDRCOTIZADOR,
    FRONTERIZO AS CDRAUTOSURTIDO,
    IDESTADO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.TALLERES`;

-- Extracción de PROVEEDOR
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_PROVEEDOR` AS
SELECT 
    CODPROVEEDOR,
    NOMBRE AS NOMBREPROVEEDOR,
    TIPO AS TIPOPROVEEDOR_PORTAL,
    IDTIPOTOT
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.PROVEEDOR`;

-- Extracción de TIPOTOT
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.DE_TIPOTOT` AS
SELECT 
    IDTIPOTOT,
    DESCRIPCION AS TIPOTOT
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.TIPOTOT`;

-- Extracción de Prestadores (BSC_SINI)
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ID_PRESTADORES_MARCA` AS
SELECT 
    Id,
    Marca,
    Status,
    Tipo,
    POBCOMER
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_verificaciones_dev.PRESTADORES`;

-- Extracción de TESTADO_BSC
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.POB_COMER` AS
SELECT 
    Z_ID,
    EDOPOB
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_verificaciones_dev.TESTADO_BSC`;

-- Extracción de tipoProveedor
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ID_TIPOPROVEEDOR` AS
SELECT 
    id,
    tipo_proveedor
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_verificaciones_dev.TIPOPROVEEDOR`;
