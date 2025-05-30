SELECT
    REPLACE(REPLACE(REPLACE(Z_ID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Z_ID,
    REPLACE(REPLACE(REPLACE(RAMO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS RAMO,
    REPLACE(REPLACE(REPLACE(SUBRAMO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS SUBRAMO,
    REPLACE(REPLACE(REPLACE(SINIESTRO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS SINIESTRO,
    REPLACE(REPLACE(REPLACE(TIPOSIN, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TIPOSIN,
    REPLACE(REPLACE(REPLACE(OF_VENTAS, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS OF_VENTAS,
    REPLACE(REPLACE(REPLACE(GERENTE, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS GERENTE,
    REPLACE(REPLACE(REPLACE(AGENTE, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS AGENTE,
    REPLACE(REPLACE(REPLACE(OF_ATEN, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS OF_ATEN,
    REPLACE(REPLACE(REPLACE(AJUSTADOR, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS AJUSTADOR,
    FECHA_REG,
    FECHA_MOV,
    REPLACE(REPLACE(REPLACE(TIPO_MOV, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TIPO_MOV,
    REPLACE(REPLACE(REPLACE(GASTO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS GASTO,
    REPLACE(REPLACE(REPLACE(MONEDA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS MONEDA,
    REPLACE(REPLACE(REPLACE(COBERTURA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS COBERTURA,
    IMPORTE,
    CSTROS,
    CRVAINICIAL,
    IRVAINICIAL,
    CAJMAS,
    IAJMAS,
    CAJMENOS,
    IAJMENOS,
    CPAGOS,
    IPAGOS,
    CGASTO,
    IGASTO,
    CDED,
    IDED,
    CREC,
    IREC,
    CSALV,
    ISALV,
    CAPUFE,
    REPLACE(REPLACE(REPLACE(COD_COBER, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS COD_COBER,
    REPLACE(REPLACE(REPLACE(COD_TEXTO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS COD_TEXTO,
    REPLACE(REPLACE(REPLACE(FLAG_REC, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS FLAG_REC,
    REPLACE(REPLACE(REPLACE(COD_RESP, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS COD_RESP,
    TIPO_CAMBIO,
    REPLACE(REPLACE(REPLACE(REPORTE, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS REPORTE,
    REPLACE(REPLACE(REPLACE(DEPRES1, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS DEPRES1,
    REPLACE(REPLACE(REPLACE(DEPRES2, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS DEPRES2,
    REPLACE(REPLACE(REPLACE(DEPRES3, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS DEPRES3,
    REPLACE(REPLACE(REPLACE(DEPRESLIT, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS DEPRESLIT,
    MONTO_INDEM_RECUP,
    MONTO_INDEM_RECUP_DM,
    IMPORTEMN,
    IRVAINICIALMN,
    IAJMASMN,
    IAJMENOSMN,
    IPAGOSMN,
    IGASTOMN,
    IDEDMN,
    IRECMN,
    ISALVMN,
    REPLACE(REPLACE(REPLACE(USUARIO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS USUARIO,
    REPLACE(REPLACE(REPLACE(POLIZA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS POLIZA,
    REPLACE(REPLACE(REPLACE(RELEVANTES, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS RELEVANTES,
    REPLACE(REPLACE(REPLACE(MARCA_RVA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS MARCA_RVA,
    COAS,
    FEC_OCU,
    REPLACE(REPLACE(REPLACE(TIPO_VEH, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TIPO_VEH,
    REPLACE(REPLACE(REPLACE(NOMASEG, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS NOMASEG,
    REPLACE(REPLACE(REPLACE(COD_POB_ACC, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS COD_POB_ACC,
    REPLACE(REPLACE(REPLACE(MODELO_VEH, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS MODELO_VEH,
    REC_CANTO,
    REPLACE(REPLACE(REPLACE(INVSALV, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS INVSALV,
    REPLACE(REPLACE(REPLACE(COD_EDO_ACC, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS COD_EDO_ACC,
    SINIESDIA,
    REPLACE(REPLACE(REPLACE(CVE_ABOGADO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS CVE_ABOGADO
FROM BSCSiniestros.dbo.RESERVAS_BSC;
