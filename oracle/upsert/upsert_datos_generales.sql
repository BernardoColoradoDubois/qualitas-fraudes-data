MERGE INTO INSUMOS.DM_DATOS_GENERALES tgt
USING RAW_INSUMOS.STG_DATOS_GENERALES stg
ON (tgt.ID = stg.ID)
WHEN MATCHED THEN
  UPDATE SET
    tgt.ID_EXPEDIENTE = stg.ID_EXPEDIENTE,
    tgt.ID_ORIGEN = stg.ID_ORIGEN,
    tgt.ID_IVA = stg.ID_IVA,
    tgt.ID_VALUACION = stg.ID_VALUACION,
    tgt.CLAVE_CAUSA_DIF = stg.CLAVE_CAUSA_DIF,
    tgt.ID_RIESGO = stg.ID_RIESGO,
    tgt.NUMERO_POLIZA = stg.NUMERO_POLIZA,
    tgt.NUMERO_ENDOSO = stg.NUMERO_ENDOSO,
    tgt.NUMERO_INCISO = stg.NUMERO_INCISO,
    tgt.NUMERO_REPORTE = stg.NUMERO_REPORTE,
    tgt.NUMERO_SINIESTRO = stg.NUMERO_SINIESTRO,
    tgt.CODIGO_RAMO = stg.CODIGO_RAMO,
    tgt.EJERCICIO = stg.EJERCICIO,
    tgt.CODIGO_SEGUIMIENTO = stg.CODIGO_SEGUIMIENTO,
    tgt.CODIGO_AFECTADO = stg.CODIGO_AFECTADO,
    tgt.NOMBRE_CONDUCTOR = stg.NOMBRE_CONDUCTOR,
    tgt.STATUS_DOCUMENTO = stg.STATUS_DOCUMENTO,
    tgt.CLAVE_TALLER = stg.CLAVE_TALLER,
    tgt.TIPO_PROVEEDOR = stg.TIPO_PROVEEDOR,
    tgt.CODIGO_VALUADOR = stg.CODIGO_VALUADOR,
    tgt.SUMA_ASEGURADA = stg.SUMA_ASEGURADA,
    tgt.PORCENTAJE_COSTOS = stg.PORCENTAJE_COSTOS,
    tgt.MONTO_DEDUCIBLE = stg.MONTO_DEDUCIBLE,
    tgt.MONTO_COSTOS = stg.MONTO_COSTOS,
    tgt.MONTO_COMP = stg.MONTO_COMP,
    tgt.MONTO_TOTS = stg.MONTO_TOTS,
    tgt.REPARACION = stg.REPARACION,
    tgt.MONTO_REPARACION = stg.MONTO_REPARACION,
    tgt.PAGO_DANOS = stg.PAGO_DANOS,
    tgt.MONTO_PD = stg.MONTO_PD,
    tgt.PERDIDA_TOTAL = stg.PERDIDA_TOTAL,
    tgt.MONTO_PT = stg.MONTO_PT,
    tgt.USUARIO = stg.USUARIO,
    tgt.ORIGEN = stg.ORIGEN,
    tgt.CLAVE_ORIGEN = stg.CLAVE_ORIGEN,
    tgt.ABONADO = stg.ABONADO,
    tgt.SISTEMA_CAPTURA = stg.SISTEMA_CAPTURA,
    tgt.SISTEMA_CLAVE = stg.SISTEMA_CLAVE,
    tgt.OBSERVACIONES = stg.OBSERVACIONES,
    tgt.TIPO_RIESGO = stg.TIPO_RIESGO,
    tgt.OFICINA = stg.OFICINA,
    tgt.REF_TALLER = stg.REF_TALLER,
    tgt.PORCENTAJE_DEDUCIBLE = stg.PORCENTAJE_DEDUCIBLE,
    tgt.OBSERVACIONES_DEDUCIBLE = stg.OBSERVACIONES_DEDUCIBLE,
    tgt.CLAVE_TALLER_ANTERIOR = stg.CLAVE_TALLER_ANTERIOR,
    tgt.AUTORIZADO_ADMIN = stg.AUTORIZADO_ADMIN,
    tgt.PRESUPUESTO_MOB = stg.PRESUPUESTO_MOB,
    tgt.PIEZAS_CAMBIO = stg.PIEZAS_CAMBIO,
    tgt.FOLIO_CABINA = stg.FOLIO_CABINA,
    tgt.OBSERVACIONES_PT = stg.OBSERVACIONES_PT,
    tgt.VEHICULO_REPARABLE = stg.VEHICULO_REPARABLE,
    tgt.ID_CAUSA_PT = stg.ID_CAUSA_PT,
    tgt.CONSECUTIVO = stg.CONSECUTIVO,
    tgt.MONTO_RESCATE_PT = stg.MONTO_RESCATE_PT,
    tgt.RESCATE_PT = stg.RESCATE_PT,
    tgt.ID_COBERTURA_TIPO = stg.ID_COBERTURA_TIPO,
    tgt.CODIGO_VALUADOR_COMPLEMENTO = stg.CODIGO_VALUADOR_COMPLEMENTO,
    tgt.FOLIO_ORDEN_ADMISION_DE = stg.FOLIO_ORDEN_ADMISION_DE,
    tgt.SOLICITANTE_DE = stg.SOLICITANTE_DE,
    tgt.PERDIDA_TOTAL_EVIDENTE = stg.PERDIDA_TOTAL_EVIDENTE,
    tgt.DEDUCIBLE_MODIFICADO = stg.DEDUCIBLE_MODIFICADO,
    tgt.PAGO_ANTICIPADO = stg.PAGO_ANTICIPADO,
    tgt.FOLIO_MO = stg.FOLIO_MO,
    tgt.AGENTE = stg.AGENTE,
    tgt.CODIGO_ASEGURADO = stg.CODIGO_ASEGURADO,
    tgt.ID_CODIGO_EVENTO = stg.ID_CODIGO_EVENTO,
    tgt.CODIGO_VALUADOR_MASTER_PT = stg.CODIGO_VALUADOR_MASTER_PT,
    tgt.CODIGO_VALUADOR_INICIAL = stg.CODIGO_VALUADOR_INICIAL,
    tgt.ID_VALUACION_INICIAL = stg.ID_VALUACION_INICIAL,
    tgt.ID_VALUACION_PT_INICIAL = stg.ID_VALUACION_PT_INICIAL,
    tgt.ESTATUS_INICIAL_ENVIO = stg.ESTATUS_INICIAL_ENVIO,
    tgt.ESTATUS_FINAL_ENVIO = stg.ESTATUS_FINAL_ENVIO,
    tgt.ESTATUS_GRUA = stg.ESTATUS_GRUA,
    tgt.DIRECCION_GRUA = stg.DIRECCION_GRUA,
    tgt.SYNC_SALVAMENTOS = stg.SYNC_SALVAMENTOS,
    tgt.CANCELA_PT = stg.CANCELA_PT,
    tgt.ESTATUS_PT = stg.ESTATUS_PT,
    tgt.GRANIZADOS = stg.GRANIZADOS,
    tgt.MOTIVO_AX = stg.MOTIVO_AX,
    tgt.SUBRAMO = stg.SUBRAMO,
    tgt.REC_CHATARRA = stg.REC_CHATARRA,
    tgt.CONV_COSTO_MEDIO = stg.CONV_COSTO_MEDIO,
    tgt.MNT_COSTO_MEDIO = stg.MNT_COSTO_MEDIO,
    tgt.MONTO_ESTIMACION_PT = stg.MONTO_ESTIMACION_PT,
    tgt.CAUSA_CODIGO = stg.CAUSA_CODIGO,
    tgt.CAUSA_DESCRIPCION = stg.CAUSA_DESCRIPCION,
    tgt.OFICINA_EMISION_CODIGO = stg.OFICINA_EMISION_CODIGO,
    tgt.OFICINA_EMISION_DESCRIPCION = stg.OFICINA_EMISION_DESCRIPCION,
    tgt.RESP_VALUADOR_CM = stg.RESP_VALUADOR_CM,
    tgt.RESPONSABLE = stg.RESPONSABLE,
    tgt.CARRIL_EXPRESS = stg.CARRIL_EXPRESS,
    tgt.RESPUESTA_COORD_CM = stg.RESPUESTA_COORD_CM,
    tgt.AUT_CON_CM = stg.AUT_CON_CM,
    tgt.RESPUESTA_VALUADOR_COMBO_CMPL_CM = stg.RESPUESTA_VALUADOR_COMBO_CMPL_CM,
    tgt.SELECC_REF_VALUADOR_CM = stg.SELECC_REF_VALUADOR_CM,
    tgt.CANCELO_REF_VALUADOR = stg.CANCELO_REF_VALUADOR,
    tgt.ID_COMPLEMENTO_CM = stg.ID_COMPLEMENTO_CM,
    tgt.ACTIVAR_FECHA_ESTENT = stg.ACTIVAR_FECHA_ESTENT,
    tgt.DANO_MENOR_DEDU = stg.DANO_MENOR_DEDU,
    tgt.CODIGO_RECUPERACION = stg.CODIGO_RECUPERACION,
    tgt.USUARIO_LIB_CHATARRA = stg.USUARIO_LIB_CHATARRA,
    tgt.MARCA_PTENPDD = stg.MARCA_PTENPDD,
    tgt.DEDUCIBLE_CRMJ = stg.DEDUCIBLE_CRMJ,
    tgt.TEL_ORDEN = stg.TEL_ORDEN,
    tgt.CORREO = stg.CORREO,
    tgt.CONDUCTOR = stg.CONDUCTOR,
    tgt.DEDU_ADMIN = stg.DEDU_ADMIN,
    tgt.FOTOS_DIGITAL_PRO = stg.FOTOS_DIGITAL_PRO
WHEN NOT MATCHED THEN
  INSERT (
    ID,
    ID_EXPEDIENTE,
    ID_ORIGEN,
    ID_IVA,
    ID_VALUACION,
    CLAVE_CAUSA_DIF,
    ID_RIESGO,
    NUMERO_POLIZA,
    NUMERO_ENDOSO,
    NUMERO_INCISO,
    NUMERO_REPORTE,
    NUMERO_SINIESTRO,
    CODIGO_RAMO,
    EJERCICIO,
    CODIGO_SEGUIMIENTO,
    CODIGO_AFECTADO,
    NOMBRE_CONDUCTOR,
    STATUS_DOCUMENTO,
    CLAVE_TALLER,
    TIPO_PROVEEDOR,
    CODIGO_VALUADOR,
    SUMA_ASEGURADA,
    PORCENTAJE_COSTOS,
    MONTO_DEDUCIBLE,
    MONTO_COSTOS,
    MONTO_COMP,
    MONTO_TOTS,
    REPARACION,
    MONTO_REPARACION,
    PAGO_DANOS,
    MONTO_PD,
    PERDIDA_TOTAL,
    MONTO_PT,
    USUARIO,
    ORIGEN,
    CLAVE_ORIGEN,
    ABONADO,
    SISTEMA_CAPTURA,
    SISTEMA_CLAVE,
    OBSERVACIONES,
    TIPO_RIESGO,
    OFICINA,
    REF_TALLER,
    PORCENTAJE_DEDUCIBLE,
    OBSERVACIONES_DEDUCIBLE,
    CLAVE_TALLER_ANTERIOR,
    AUTORIZADO_ADMIN,
    PRESUPUESTO_MOB,
    PIEZAS_CAMBIO,
    FOLIO_CABINA,
    OBSERVACIONES_PT,
    VEHICULO_REPARABLE,
    ID_CAUSA_PT,
    CONSECUTIVO,
    MONTO_RESCATE_PT,
    RESCATE_PT,
    ID_COBERTURA_TIPO,
    CODIGO_VALUADOR_COMPLEMENTO,
    FOLIO_ORDEN_ADMISION_DE,
    SOLICITANTE_DE,
    PERDIDA_TOTAL_EVIDENTE,
    DEDUCIBLE_MODIFICADO,
    PAGO_ANTICIPADO,
    FOLIO_MO,
    AGENTE,
    CODIGO_ASEGURADO,
    ID_CODIGO_EVENTO,
    CODIGO_VALUADOR_MASTER_PT,
    CODIGO_VALUADOR_INICIAL,
    ID_VALUACION_INICIAL,
    ID_VALUACION_PT_INICIAL,
    ESTATUS_INICIAL_ENVIO,
    ESTATUS_FINAL_ENVIO,
    ESTATUS_GRUA,
    DIRECCION_GRUA,
    SYNC_SALVAMENTOS,
    CANCELA_PT,
    ESTATUS_PT,
    GRANIZADOS,
    MOTIVO_AX,
    SUBRAMO,
    REC_CHATARRA,
    CONV_COSTO_MEDIO,
    MNT_COSTO_MEDIO,
    MONTO_ESTIMACION_PT,
    CAUSA_CODIGO,
    CAUSA_DESCRIPCION,
    OFICINA_EMISION_CODIGO,
    OFICINA_EMISION_DESCRIPCION,
    RESP_VALUADOR_CM,
    RESPONSABLE,
    CARRIL_EXPRESS,
    RESPUESTA_COORD_CM,
    AUT_CON_CM,
    RESPUESTA_VALUADOR_COMBO_CMPL_CM,
    SELECC_REF_VALUADOR_CM,
    CANCELO_REF_VALUADOR,
    ID_COMPLEMENTO_CM,
    ACTIVAR_FECHA_ESTENT,
    DANO_MENOR_DEDU,
    CODIGO_RECUPERACION,
    USUARIO_LIB_CHATARRA,
    MARCA_PTENPDD,
    DEDUCIBLE_CRMJ,
    TEL_ORDEN,
    CORREO,
    CONDUCTOR,
    DEDU_ADMIN,
    FOTOS_DIGITAL_PRO
  )
  VALUES (
    stg.ID,
    stg.ID_EXPEDIENTE,
    stg.ID_ORIGEN,
    stg.ID_IVA,
    stg.ID_VALUACION,
    stg.CLAVE_CAUSA_DIF,
    stg.ID_RIESGO,
    stg.NUMERO_POLIZA,
    stg.NUMERO_ENDOSO,
    stg.NUMERO_INCISO,
    stg.NUMERO_REPORTE,
    stg.NUMERO_SINIESTRO,
    stg.CODIGO_RAMO,
    stg.EJERCICIO,
    stg.CODIGO_SEGUIMIENTO,
    stg.CODIGO_AFECTADO,
    stg.NOMBRE_CONDUCTOR,
    stg.STATUS_DOCUMENTO,
    stg.CLAVE_TALLER,
    stg.TIPO_PROVEEDOR,
    stg.CODIGO_VALUADOR,
    stg.SUMA_ASEGURADA,
    stg.PORCENTAJE_COSTOS,
    stg.MONTO_DEDUCIBLE,
    stg.MONTO_COSTOS,
    stg.MONTO_COMP,
    stg.MONTO_TOTS,
    stg.REPARACION,
    stg.MONTO_REPARACION,
    stg.PAGO_DANOS,
    stg.MONTO_PD,
    stg.PERDIDA_TOTAL,
    stg.MONTO_PT,
    stg.USUARIO,
    stg.ORIGEN,
    stg.CLAVE_ORIGEN,
    stg.ABONADO,
    stg.SISTEMA_CAPTURA,
    stg.SISTEMA_CLAVE,
    stg.OBSERVACIONES,
    stg.TIPO_RIESGO,
    stg.OFICINA,
    stg.REF_TALLER,
    stg.PORCENTAJE_DEDUCIBLE,
    stg.OBSERVACIONES_DEDUCIBLE,
    stg.CLAVE_TALLER_ANTERIOR,
    stg.AUTORIZADO_ADMIN,
    stg.PRESUPUESTO_MOB,
    stg.PIEZAS_CAMBIO,
    stg.FOLIO_CABINA,
    stg.OBSERVACIONES_PT,
    stg.VEHICULO_REPARABLE,
    stg.ID_CAUSA_PT,
    stg.CONSECUTIVO,
    stg.MONTO_RESCATE_PT,
    stg.RESCATE_PT,
    stg.ID_COBERTURA_TIPO,
    stg.CODIGO_VALUADOR_COMPLEMENTO,
    stg.FOLIO_ORDEN_ADMISION_DE,
    stg.SOLICITANTE_DE,
    stg.PERDIDA_TOTAL_EVIDENTE,
    stg.DEDUCIBLE_MODIFICADO,
    stg.PAGO_ANTICIPADO,
    stg.FOLIO_MO,
    stg.AGENTE,
    stg.CODIGO_ASEGURADO,
    stg.ID_CODIGO_EVENTO,
    stg.CODIGO_VALUADOR_MASTER_PT,
    stg.CODIGO_VALUADOR_INICIAL,
    stg.ID_VALUACION_INICIAL,
    stg.ID_VALUACION_PT_INICIAL,
    stg.ESTATUS_INICIAL_ENVIO,
    stg.ESTATUS_FINAL_ENVIO,
    stg.ESTATUS_GRUA,
    stg.DIRECCION_GRUA,
    stg.SYNC_SALVAMENTOS,
    stg.CANCELA_PT,
    stg.ESTATUS_PT,
    stg.GRANIZADOS,
    stg.MOTIVO_AX,
    stg.SUBRAMO,
    stg.REC_CHATARRA,
    stg.CONV_COSTO_MEDIO,
    stg.MNT_COSTO_MEDIO,
    stg.MONTO_ESTIMACION_PT,
    stg.CAUSA_CODIGO,
    stg.CAUSA_DESCRIPCION,
    stg.OFICINA_EMISION_CODIGO,
    stg.OFICINA_EMISION_DESCRIPCION,
    stg.RESP_VALUADOR_CM,
    stg.RESPONSABLE,
    stg.CARRIL_EXPRESS,
    stg.RESPUESTA_COORD_CM,
    stg.AUT_CON_CM,
    stg.RESPUESTA_VALUADOR_COMBO_CMPL_CM,
    stg.SELECC_REF_VALUADOR_CM,
    stg.CANCELO_REF_VALUADOR,
    stg.ID_COMPLEMENTO_CM,
    stg.ACTIVAR_FECHA_ESTENT,
    stg.DANO_MENOR_DEDU,
    stg.CODIGO_RECUPERACION,
    stg.USUARIO_LIB_CHATARRA,
    stg.MARCA_PTENPDD,
    stg.DEDUCIBLE_CRMJ,
    stg.TEL_ORDEN,
    stg.CORREO,
    stg.CONDUCTOR,
    stg.DEDU_ADMIN,
    stg.FOTOS_DIGITAL_PRO
  );