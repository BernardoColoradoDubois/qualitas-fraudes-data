MERGE INTO INSUMOS.DM_COBERTURAS tgt
USING RAW_INSUMOS.STG_COBERTURAS stg
ON (tgt.ID = stg.ID)
WHEN MATCHED THEN
  UPDATE SET
    tgt.COBERTURA = stg.COBERTURA,
    tgt.COBERTURA_HOMOLOGADA = stg.COBERTURA_HOMOLOGADA
WHEN NOT MATCHED THEN
  INSERT (
    ID,
    COBERTURA,
    COBERTURA_HOMOLOGADA
  )
  VALUES (
    stg.ID,
    stg.COBERTURA,
    stg.COBERTURA_HOMOLOGADA
  )