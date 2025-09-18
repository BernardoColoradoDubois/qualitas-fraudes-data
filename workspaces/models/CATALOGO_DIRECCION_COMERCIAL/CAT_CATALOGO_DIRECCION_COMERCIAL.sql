SELECT 
  -- NO_OF: Integer -> INT64
  SAFE_CAST(NO_OF AS INT64) AS NO_OF,
  
  -- OFICINA: String -> STRING (mantiene como string)
  OFICINA,
  
  -- ZONA_ATENCION: String -> STRING (mantiene como string)
  ZONA_ATENCION,
  
  -- DIRECTOR_GENERAL: String -> STRING (mantiene como string)
  DIRECTOR_GENERAL,
  
  -- SUBDIRECTOR_GENERAL: String -> STRING (mantiene como string)
  SUBDIRECTOR_GENERAL

FROM `qlts-dev-mx-au-bro-verificacio.LAN_VERIFICACIONES.CATALOGO_DIRECCION_COMERCIAL`;