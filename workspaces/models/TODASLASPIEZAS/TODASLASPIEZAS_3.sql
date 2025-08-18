
-- FASE 3: CONSOLIDACIÓN DE INFORMACIÓN DE PRESTADORES
-- ================================================================================

-- Unión de prestadores con tipos y poblaciones
CREATE OR REPLACE TABLE `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.PRESTADORES_POB_COMER_TIPO` AS
SELECT 
    t1.Id,
    t3.EDOPOB AS Pob_Comer,
    t1.Marca,
    t2.tipo_proveedor,
    t1.Status
FROM `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.ID_PRESTADORES_MARCA` t1
INNER JOIN `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.ID_TIPOPROVEEDOR` t2 ON t1.Tipo = t2.id
LEFT JOIN `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.POB_COMER` t3 ON t1.POBCOMER = t3.Z_ID;

-- Unión de proveedor con tipotot y prestadores
CREATE OR REPLACE TABLE `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.QUERY_FOR_PROVEEDOR` AS
SELECT 
    t1.CODPROVEEDOR,
    t1.NOMBREPROVEEDOR,
    t1.TIPOPROVEEDOR_PORTAL,
    t2.TIPOTOT,
    t3.Pob_Comer AS POBCOMERCIALPROVEEDOR,
    t3.Marca AS MARCAPROVEEDOR_SISE,
    t3.tipo_proveedor AS TIPOPROVEEDOR_SISE
FROM `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.DE_PROVEEDOR` t1
LEFT JOIN `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.DE_TIPOTOT` t2 ON t1.IDTIPOTOT = t2.IDTIPOTOT
LEFT JOIN `qlts-dev-mx-au-bro-verificacio.STG_PREVENCION_FRAUDES.PRESTADORES_POB_COMER_TIPO` t3 ON t1.CODPROVEEDOR = t3.Id;

