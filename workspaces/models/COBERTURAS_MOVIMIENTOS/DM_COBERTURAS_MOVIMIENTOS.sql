CREATE OR REPLACE TABLE `DM_FRAUDES.DM_COBERTURAS_MOVIMIENTOS` AS
SELECT 
Z_ID AS ID,
RAMO,
SUBRAMO,
SINIESTRO,
TIPOSIN AS TIPO_SINIESTRO,
OF_VENTAS as ID_OFICINA_VENTAS,
GERENTE,
AGENTE,
OF_ATEN AS ID_OFICINA_DE_ATENCION,
AJUSTADOR,
FECHA_REG AS FECHA_REGISTRO,
FECHA_MOV as FECHA_MOVIMIENTO,
TIPO_MOV AS TIPO_MOVIMIENTO,
GASTO,
MONEDA AS TIPO_MONEDA,
COBERTURA,
IMPORTE,
CAPUFE,
COD_COBER AS CODIGO_COBERTURA,
COD_TEXTO AS CODIGO_TEXTO,
TIPO_CAMBIO,
REPORTE,
USUARIO,
POLIZA,
RELEVANTES,
FEC_OCU AS FECHA_OCURRIDO,
TIPO_VEH AS TIPO_VEHICULO,
MODELO_VEH AS MODELO_VEHICULO,
CSTROS,
CRVAINICIAL AS CRVA_INICIAL,
IRVAINICIAL AS IRVA_INICIAL,
CAJMAS,
IAJMAS,
CAJMENOS,
IAJMENOS,
CPAGOS,
IPAGOS,
CGASTO,
IGASTO,
CDED,
IREC,
CSALV,
ISALV,
FLAG_REC,
COD_RESP,
DEPRES1,
DEPRES2,
DEPRES3,
DEPRESLIT,
MONTO_INDEM_RECUP,
MONTO_INDEM_RECUP_DM,
IMPORTEMN as IMPORTE_MN,
IRVAINICIALMN as IRVA_INICIAL_MN,
IAJMASMN,
IAJMENOSMN,
IPAGOSMN,
IGASTOMN,
IDEDMN,
IRECMN,
ISALVMN,
MARCA_RVA,
COAS,
NOMASEG as NOMBRE_ASEGURADORA,
COD_POB_ACC AS CODIGO_POBLACION_ACC,
REC_CANTO,
INVSALV,
COD_EDO_ACC as CODIGO_ESTADO_ACC,
SINIESDIA,
CVE_ABOGADO AS CLAVE_ABOGADO
FROM `qualitasfraude.sample_landing_siniestros_bsc.reservas_bsc`;