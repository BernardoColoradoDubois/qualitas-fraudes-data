SELECT
    REPLACE(REPLACE(REPLACE(RECIBO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RECIBO,
    REPLACE(REPLACE(REPLACE(FEC_VTO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_VTO,
    REPLACE(REPLACE(REPLACE(POLIZA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POLIZA,
    REPLACE(REPLACE(REPLACE(ENDOSO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ENDOSO,
    REPLACE(REPLACE(REPLACE(IMPORTE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS IMPORTE,
    REPLACE(REPLACE(REPLACE(REMESA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS REMESA,
    REPLACE(REPLACE(REPLACE(MARCA_IMP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_IMP,
    REPLACE(REPLACE(REPLACE(MARCA_ANUL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_ANUL,
    REPLACE(REPLACE(REPLACE(PRIMA_NETA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PRIMA_NETA,
    REPLACE(REPLACE(REPLACE(DER_POL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DER_POL,
    REPLACE(REPLACE(REPLACE(REC_FIN, CHR(10), ''), CHR(13), ''), CHR(9), '') AS REC_FIN,
    REPLACE(REPLACE(REPLACE(IVA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS IVA,
    REPLACE(REPLACE(REPLACE(BONIFICACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BONIFICACION,
    REPLACE(REPLACE(REPLACE(BONIF_RF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BONIF_RF,
    REPLACE(REPLACE(REPLACE(BONIF_TEC, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BONIF_TEC,
    REPLACE(REPLACE(REPLACE(PRIMA_TOTAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PRIMA_TOTAL,
    REPLACE(REPLACE(REPLACE(USU_APLICA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS USU_APLICA,
    REPLACE(REPLACE(REPLACE(FEC_APLICACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_APLICACION,
    REPLACE(REPLACE(REPLACE(HORA_APLICA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS HORA_APLICA,
    REPLACE(REPLACE(REPLACE(IMPRESION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS IMPRESION,
    REPLACE(REPLACE(REPLACE(RFC_FACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RFC_FACT,
    REPLACE(REPLACE(REPLACE(USUARIO_EMITE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS USUARIO_EMITE,
    REPLACE(REPLACE(REPLACE(FECHA_PROCESO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FECHA_PROCESO,
    FECHA_CARGA
FROM 
    SASFRAAP.FRAUD_RP;