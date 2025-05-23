TRUNCATE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}`;
INSERT INTO `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}`
SELECT DISTINCT
  Z_ID as ID,
  REPORTE,
  POLIZA,
  ENDOSO,
  INCISO,
  CONCAT(POLIZA, ENDOSO) AS ID_POLIZA_ENDOSO,
  CONCAT(POLIZA, ENDOSO, INCISO) AS ID_POLIZA_ENDOSO_INCISO,
  ASEGURADO,
  ID_ASEG AS ID_ASEGURADO,
  CAUSA AS ID_CAUSA,
  HOR_OCU AS FECHA_OCURRIDO,
  HOR_REP AS FECHA_REPORTE,
  HOR_REG AS FECHA_REGISTRO,
  FEC_INI_VIG AS FECHA_INICIO_VIGENCIA,
  MARCA,
  MODELO,
  SERIE,
  SEVERIDAD,
  ESTATUS AS STATUS,
  RESERVA,
  UBICACION,
  ENTIDAD,
  OFICINA AS ID_OFICINA,
  LEIDO_SAS,
  OFICINA_DE_ATENCION AS ID_OFICINA_DE_ATENCION,
  ID_AJUSTADOR,
  LATITUD,
  LONGITUD,
  CONDUCTOR,
  COD_USO AS CODIGO_USO,
  USO,
  SUMA_ASEG AS SUMA_ASEGURADA,
  PLACA,
  FEC_EMI AS FECHA_EMISION,
  CASE 
    WHEN POL_REHAB IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS POLIZA_REHABILITADA,
  FEC_FIN_VIG AS FECHA_FIN_VIGENCIA,
  OBSERVACIONES,
  BATCHDATE AS FECHA_LOTE,
  SYSUSERID AS ID_USUARIO_SISTEMA
FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`