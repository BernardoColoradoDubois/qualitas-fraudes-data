

-- ================================================================================
-- FASE 7: CÁLCULO DE TIEMPOS DE ENTREGA
-- ================================================================================

-- Fechas mínimas y máximas por expediente-proveedor-pieza
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.FECEXP_FECENTREGA_TE` AS
SELECT DISTINCT 
    t1.IDEXPEDIENTE,
    t1.CODPROVEEDOR,
    CONCAT(IFNULL(t1.NUMPARTE,''), IFNULL(t1.REFERENCIA,''), IFNULL(t1.DESCRIPCIONREFACCION,'')) AS LLAVEPIEZA,
    MIN(t1.FECHAEXPEDICION) AS MIN_of_FECHAEXPEDICION,
    MAX(t1.FECENTREGAREFACCIONARIA) AS MAX_of_FECENTREGAREFACCIONARIA,
    t1.NUMPARTE,
    t1.REFERENCIA,
    t1.DESCRIPCIONREFACCION
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.APPEND_TABLE` t1
GROUP BY 
    t1.IDEXPEDIENTE,
    t1.CODPROVEEDOR,
    LLAVEPIEZA,
    t1.NUMPARTE,
    t1.REFERENCIA,
    t1.DESCRIPCIONREFACCION;

-- Cálculo de tiempo de entrega
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.TE` AS
SELECT 
    t1.IDEXPEDIENTE,
    t1.CODPROVEEDOR,
    t1.LLAVEPIEZA,
    DATE_DIFF(DATE(t1.MAX_of_FECENTREGAREFACCIONARIA), DATE(t1.MIN_of_FECHAEXPEDICION), DAY) AS TIEMPOENTREGA,
    t1.NUMPARTE,
    t1.REFERENCIA,
    t1.DESCRIPCIONREFACCION
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.FECEXP_FECENTREGA_TE` t1;

-- Tabla final con tiempos de entrega
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.APPEND_TABLE_TE` AS
SELECT 
    t1.IDEXPEDIENTE,
    t1.IDVALEHISTORICO,
    t1.IDVALE,
    t1.CODPROVEEDOR,
    t1.NOMBREPROVEEDOR,
    t1.TIPOPROVEEDOR_PORTAL,
    t1.TIPOTOT,
    t1.POBCOMERCIALPROVEEDOR,
    t1.TIPOPROVEEDOR_SISE,
    t1.MARCAPROVEEDOR_SISE,
    t1.FOLIO,
    t1.NUMPARTE,
    t1.REFERENCIA,
    t1.DESCRIPCIONREFACCION,
    t1.MONTO,
    t1.IDCOSTO,
    t1.TIPO,
    t1.FECHAEXPEDICION,
    t1.FECENVIO,
    t1.FECENTREGAREFACCIONARIA,
    t1.FECRECEPCION,
    t1.FECPROMESAENTREGA,
    t1.FECHAACTUALIZACION,
    t1.USUARIO,
    t1.AUTORIZADOR,
    t1.ESTATUSVALE,
    t1.ORIGEN,
    t1.CAUSACAMBIOVALE,
    t1.LISTA,
    t1.BLINDAJE,
    t1.CLAVENAGS,
    t1.MONTOCONVENIO,
    t2.TIEMPOENTREGA,
    DATE_DIFF(DATE(t1.FECENTREGAREFACCIONARIA), DATE(t1.FECHAEXPEDICION), DAY) AS TIEMPOENTREGA1,
    DATE_DIFF(DATE(t1.FECRECEPCION), DATE(t1.FECHAEXPEDICION), DAY) AS TIEMPORECEPCION
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.APPEND_TABLE` t1
LEFT JOIN `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.TE` t2 ON 
    t1.IDEXPEDIENTE = t2.IDEXPEDIENTE 
    AND t1.CODPROVEEDOR = t2.CODPROVEEDOR 
    AND t1.NUMPARTE = t2.NUMPARTE 
    AND t1.REFERENCIA = t2.REFERENCIA 
    AND t1.DESCRIPCIONREFACCION = t2.DESCRIPCIONREFACCION;

-- Unión final con información del expediente
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.UNION_01` AS
SELECT 
    a.*,
    b.NUMVALUACION,
    b.MIN_of_FECHAAUTORIZACIONVALUADOR,
    b.EJERCICIO,
    b.NUMREPORTE,
    b.NUMSINIESTRO,
    b.ESTATUSEXPEDIENTE,
    b.ESTATUSVALUACION,
    b.MARCAVEHICULO,
    b.TIPOVEHICULO,
    b.MODELO,
    b.SERIE,
    b.CLAVETALLER,
    b.NOMBRECDR,
    b.TIPOCDR_SISE,
    b.MARCACDR_SISE,
    b.TIPOCDR_PORTAL,
    b.POBCOMERCDR,
    b.IDREGIONGEOGRAFICA,
    b.CDRCOTIZADOR,
    b.CDRAUTOSURTIDO,
    b.GERENCIAVALUACION,
    b.CERCO,
    b.CERCO_ANTERIOR,
    b.CODVALUADOR,
    b.FECVALUACION,
    b.FECTERMINADO,
    b.FECENTREGADO,
    b.VEHICULOTERMINADO,
    b.TIPOVALUADOR,
    b.CATEGORIAVALUADOR,
    b.ANALISTACDR
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.APPEND_TABLE_TE` a
LEFT JOIN `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ENVIOHISTORICO_CN_ANALISTACDR` b ON a.IDEXPEDIENTE = b.IDEXPEDIENTE;

