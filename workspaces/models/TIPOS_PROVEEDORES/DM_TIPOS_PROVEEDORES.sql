CREATE OR REPLACE TABLE `DM_FRAUDES.DM_TIPOS_PROVEEDORES`  AS
SELECT
id as ID,
tipo_proveedor as TIPO_PROVEEDOR,
Grupo as GRUPO,
nombre_grupo as NOMBRE_GRUPO
FROM `qualitasfraude.sample_landing_siniestros_bsc.tipoproveedor`