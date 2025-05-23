CREATE TABLE `{{task.params.PROJECT_ID}}.{{task.params.DATASET_NAME}}.{{task.params.TABLE_NAME}}`
(
  ID STRING,
  RAMO STRING,
  SUBRAMO STRING,
  SINIESTRO STRING,
  TIPO_SINIESTRO STRING,
  ID_OFICINA_VENTAS STRING,
  GERENTE STRING,
  AGENTE STRING,
  ID_OFICINA_DE_ATENCION STRING,
  AJUSTADOR STRING,
  FECHA_REGISTRO DATETIME,
  FECHA_MOVIMIENTO DATETIME,
  TIPO_MOVIMIENTO STRING,
  GASTO STRING,
  TIPO_MONEDA STRING,
  COBERTURA STRING,
  IMPORTE FLOAT64,
  CAPUFE INT64,
  CODIGO_COBERTURA STRING,
  CODIGO_TEXTO STRING,
  TIPO_CAMBIO FLOAT64,
  REPORTE STRING,
  USUARIO STRING,
  POLIZA STRING,
  RELEVANTES STRING,
  FECHA_OCURRIDO DATETIME,
  TIPO_VEHICULO STRING,
  MODELO_VEHICULO STRING,
  CSTROS INT64,
  CRVA_INICIAL INT64,
  IRVA_INICIAL FLOAT64,
  CAJMAS INT64,
  IAJMAS FLOAT64,
  CAJMENOS INT64,
  IAJMENOS FLOAT64,
  CPAGOS INT64,
  IPAGOS FLOAT64,
  CGASTO INT64,
  IGASTO FLOAT64,
  CDED INT64,
  IREC FLOAT64,
  CSALV INT64,
  ISALV FLOAT64,
  FLAG_REC STRING,
  COD_RESP STRING,
  DEPRES1 STRING,
  DEPRES2 STRING,
  DEPRES3 STRING,
  DEPRESLIT STRING,
  MONTO_INDEM_RECUP FLOAT64,
  MONTO_INDEM_RECUP_DM FLOAT64,
  IMPORTE_MN FLOAT64,
  IRVA_INICIAL_MN FLOAT64,
  IAJMASMN FLOAT64,
  IAJMENOSMN FLOAT64,
  IPAGOSMN FLOAT64,
  IGASTOMN FLOAT64,
  IDEDMN FLOAT64,
  IRECMN FLOAT64,
  ISALVMN FLOAT64,
  MARCA_RVA STRING,
  COAS FLOAT64,
  NOMBRE_ASEGURADORA STRING,
  CODIGO_POBLACION_ACC STRING,
  REC_CANTO FLOAT64,
  INVSALV STRING,
  CODIGO_ESTADO_ACC STRING,
  SINIESDIA INT64,
  CLAVE_ABOGADO STRING
);
