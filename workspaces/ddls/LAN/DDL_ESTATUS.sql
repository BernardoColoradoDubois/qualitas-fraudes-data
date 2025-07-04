CREATE TABLE `qlts-dev-mx-au-bro-verificacio.LAN_VERIFICACIONES.ESTATUS`
(
  IDESTATUS STRING,
  IDEXPEDIENTE STRING,
  IDESTATUSEXPEDIENTE STRING,
  IDRECHAZO STRING,
  IDPENDIENTE STRING,
  TRANSITO NUMERIC,
  PISO NUMERIC,
  GRUA NUMERIC,
  FECESTATUSEXPEDIENTE DATETIME,
  MANUAL NUMERIC,
  SISE BIGNUMERIC,
  ENVIOSSISE BIGNUMERIC,
  SISEMENSAJE STRING,
  COTIZADOR BIGNUMERIC,
  COTIZADORESTATUS STRING,
  AUTORIZACIONCOORDINADOR BIGNUMERIC,
  AUTORIZACIONUSUARIO STRING,
  PRESUPUESTOENHERRAMIENTA BIGNUMERIC,
  ADMINREF STRING,
  REASIGNADOVALUADOR BIGNUMERIC,
  IDMOVOPERADOR STRING,
  IDRECHAZOCOMPLEMENTO STRING,
  CAMBIOREGIONEXP STRING,
  PROCESOHERRAMIENTA BIGNUMERIC,
  VECESRECHAZADO BIGNUMERIC,
  PAGOANTICIPADO BIGNUMERIC,
  REVISIONMASTERPT STRING,
  IDRESCATEMASTERPT STRING,
  REVISIONBLINDAJES STRING,
  ENVIOSHERRAMIENTA BIGNUMERIC,
  HISTOSINSINIESTRO STRING,
  HISTOINVESTIGACION STRING,
  FECSINSINIESTRO DATETIME,
  FECINVESTIGACION DATETIME,
  HISTOACTUALIZACIONSINIESTRO DATETIME,
  REGRESOVALEP STRING,
  FECHACANCELACION DATETIME,
  ISPIEZASCANCELADAS STRING,
  XNUUP BIGNUMERIC
);
