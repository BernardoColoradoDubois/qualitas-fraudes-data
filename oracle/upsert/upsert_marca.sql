MERGE INTO INSUMOS.DM_MARCA tgt
USING RAW_INSUMOS.STG_MARCA stg
ON (tgt.IDMARCA = stg.IDMARCA)
WHEN MATCHED THEN
  UPDATE SET
    tgt.DESCRIPCION = stg.DESCRIPCION,
    tgt.ALTA = stg.ALTA,
    tgt.ACTUALIZACION = stg.ACTUALIZACION,
    tgt.USUARIOACTUALIZA = stg.USUARIOACTUALIZA,
    tgt.RESCATEPT = stg.RESCATEPT,
    tgt.APLICACONVENIO = stg.APLICACONVENIO,
    tgt.TIEMPOMARCA = stg.TIEMPOMARCA,
    tgt.AUTOS = stg.AUTOS,
    tgt.EP = stg.EP,
    tgt.CHECKCALFECHAEE = stg.CHECKCALFECHAEE,
    tgt.V0 = stg.V0,
    tgt.V1 = stg.V1,
    tgt.V2 = stg.V2,
    tgt.V3 = stg.V3,
    tgt.V4 = stg.V4,
    tgt.PORCENTAJE_V0 = stg.PORCENTAJE_V0,
    tgt.PORCENTAJE_V1 = stg.PORCENTAJE_V1,
    tgt.PORCENTAJE_V2 = stg.PORCENTAJE_V2,
    tgt.PORCENTAJE_V3 = stg.PORCENTAJE_V3,
    tgt.PORCENTAJE_V4 = stg.PORCENTAJE_V4,
    tgt.ACTIVAMARCAPT = stg.ACTIVAMARCAPT,
    tgt.VESPECIALIZADO = stg.VESPECIALIZADO,
    tgt.ALTAESPECIALIZADOMM = stg.ALTAESPECIALIZADOMM,
    tgt.ALTAESPECIALIZADO = stg.ALTAESPECIALIZADO
WHEN NOT MATCHED THEN
  INSERT (
    IDMARCA,
    DESCRIPCION,
    ALTA,
    ACTUALIZACION,
    USUARIOACTUALIZA,
    RESCATEPT,
    APLICACONVENIO,
    TIEMPOMARCA,
    AUTOS,
    EP,
    CHECKCALFECHAEE,
    V0,
    V1,
    V2,
    V3,
    V4,
    PORCENTAJE_V0,
    PORCENTAJE_V1,
    PORCENTAJE_V2,
    PORCENTAJE_V3,
    PORCENTAJE_V4,
    ACTIVAMARCAPT,
    VESPECIALIZADO,
    ALTAESPECIALIZADOMM,
    ALTAESPECIALIZADO
  )
  VALUES (
    stg.IDMARCA,
    stg.DESCRIPCION,
    stg.ALTA,
    stg.ACTUALIZACION,
    stg.USUARIOACTUALIZA,
    stg.RESCATEPT,
    stg.APLICACONVENIO,
    stg.TIEMPOMARCA,
    stg.AUTOS,
    stg.EP,
    stg.CHECKCALFECHAEE,
    stg.V0,
    stg.V1,
    stg.V2,
    stg.V3,
    stg.V4,
    stg.PORCENTAJE_V0,
    stg.PORCENTAJE_V1,
    stg.PORCENTAJE_V2,
    stg.PORCENTAJE_V3,
    stg.PORCENTAJE_V4,
    stg.ACTIVAMARCAPT,
    stg.VESPECIALIZADO,
    stg.ALTAESPECIALIZADOMM,
    stg.ALTAESPECIALIZADO
  )