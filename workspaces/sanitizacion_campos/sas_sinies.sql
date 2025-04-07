-- siniestros
SELECT
  REPLACE(REPLACE(REPLACE(Z_ID, CHR(10), ''), CHR(13), ''), CHR(9), '') AS Z_ID,
  REPLACE(REPLACE(REPLACE(REPORTE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS REPORTE,
  REPLACE(REPLACE(REPLACE(POLIZA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POLIZA,
  REPLACE(REPLACE(REPLACE(ENDOSO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ENDOSO,
  REPLACE(REPLACE(REPLACE(INCISO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS INCISO,
  REPLACE(REPLACE(REPLACE(ASEGURADO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ASEGURADO,
  REPLACE(REPLACE(REPLACE(ID_ASEG, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ID_ASEG,
  REPLACE(REPLACE(REPLACE(CAUSA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CAUSA,
  FEC_OCU,
  HOR_OCU,
  FEC_REP,
  HOR_REP,
  FEC_REG,
  HOR_REG,
  FEC_INI_VIG,
  REPLACE(REPLACE(REPLACE(MARCA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA,
  REPLACE(REPLACE(REPLACE(MODELO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MODELO,
  REPLACE(REPLACE(REPLACE(SERIE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SERIE,
  SEVERIDAD,
  REPLACE(REPLACE(REPLACE(ESTATUS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ESTATUS,
  RESERVA,
  REPLACE(REPLACE(REPLACE(UBICACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS UBICACION,
  REPLACE(REPLACE(REPLACE(ENTIDAD, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ENTIDAD,
  REPLACE(REPLACE(REPLACE(OFICINA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS OFICINA,
  REPLACE(REPLACE(REPLACE(LEIDO_SAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS LEIDO_SAS,
  REPLACE(REPLACE(REPLACE(OFICINA_DE_ATENCION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS OFICINA_DE_ATENCION,
  REPLACE(REPLACE(REPLACE(ID_AJUSTADOR, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ID_AJUSTADOR,
  REPLACE(REPLACE(REPLACE(LATITUD, CHR(10), ''), CHR(13), ''), CHR(9), '') AS LATITUD,
  REPLACE(REPLACE(REPLACE(LONGITUD, CHR(10), ''), CHR(13), ''), CHR(9), '') AS LONGITUD,
  REPLACE(REPLACE(REPLACE(CONDUCTOR, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CONDUCTOR,
  REPLACE(REPLACE(REPLACE(COD_USO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_USO,
  REPLACE(REPLACE(REPLACE(USO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS USO,
  SUMA_ASEG,
  REPLACE(REPLACE(REPLACE(PLACA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PLACA,
  FEC_EMI,
  REPLACE(REPLACE(REPLACE(POL_REHAB, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POL_REHAB,
  FEC_FIN_VIG,
  REPLACE(REPLACE(REPLACE(OBSERVACIONES, CHR(10), ''), CHR(13), ''), CHR(9), '') AS OBSERVACIONES,
  BATCHDATE,
  REPLACE(REPLACE(REPLACE(SYSUSERID, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SYSUSERID
FROM
  SASMXAP.SAS_SINIES;

