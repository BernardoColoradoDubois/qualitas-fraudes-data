CREATE OR REPLACE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}` AS
SELECT 
  -- NO_OPERACION: String
  NO_OPERACION,
  
  -- SUCURSAL: String -> STRING (mantiene como string)
  SUCURSAL,
  
  -- REFERENCIA: String -> STRING (mantiene como string)
  REFERENCIA,
  
  -- USUARIO: String -> STRING (mantiene como string)
  USUARIO,
  
  -- USUARIO_TRANSACCION: String -> STRING (mantiene como string)
  USUARIO_TRANSACCION,
  
  -- TIPO_DE_PAGO: String -> STRING (mantiene como string)
  TIPO_DE_PAGO,
  
  -- NO_TARJETA: String -> STRING (mantiene como string)
  NO_TARJETA,
  
  -- NOMBRE_DEL_CLIENTE: String -> STRING (mantiene como string)
  NOMBRE_DEL_CLIENTE,
  
  -- AUTORIZACION: String -> STRING (mantiene como string)
  AUTORIZACION,
  
  -- TIPO_DE_TRANSACCION: String -> STRING (mantiene como string)
  TIPO_DE_TRANSACCION,
  
  -- AFILIACION: String
  AFILIACION,
  
  -- NOMBRE_COMERCIAL: String -> STRING (mantiene como string)
  NOMBRE_COMERCIAL,
  
  -- IMPORTE: Float -> FLOAT64
  SAFE_CAST(IMPORTE AS FLOAT64) AS IMPORTE,
  
  -- MONEDA: String -> STRING (mantiene como string)
  MONEDA,
  
  -- TIPO_DE_TARJETA: String -> STRING (mantiene como string)
  TIPO_DE_TARJETA,
  
  -- TARJETA: String -> STRING (mantiene como string)
  TARJETA,
  
  -- BANCO_EMISOR: String -> STRING (mantiene como string)
  BANCO_EMISOR,
  
  -- ESTATUS: String -> STRING (mantiene como string)
  ESTATUS,
  
  -- RESPUESTA: String -> STRING (mantiene como string)
  RESPUESTA,
  
  -- OPERACION: String -> STRING (mantiene como string)
  OPERACION,
  
  -- BANCO_ADQUIRENTE: String -> STRING (mantiene como string)
  BANCO_ADQUIRENTE,
  
  -- TRESDSECURE: String -> STRING (mantiene como string)
  TRESDSECURE,
  
  -- FECHA_HORA: Date -> DATETIME
  SAFE_CAST(FECHA_HORA AS DATETIME) AS FECHA_HORA

FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`;