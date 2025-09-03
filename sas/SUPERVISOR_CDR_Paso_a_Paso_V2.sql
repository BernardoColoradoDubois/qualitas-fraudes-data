

/* Librerias */
/* Library assignment for SASApp.BSC_SINI */
Libname BSC_SINI META  REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='BSC SINIESTROS'][DeployedComponents/ServerContext[@Name='SASApp']]";
/* Library assignment for SASApp.VALPROD */
Libname VALPROD META  REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Val Prod'][DeployedComponents/ServerContext[@Name='SASApp']]";
/* Library assignment for SASApp.CONVENB */
Libname CONVENB META  REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Convenios Base'][DeployedComponents/ServerContext[@Name='SASApp']]" METAOUT=DATA;
/* Library assignment for SASApp.INDBASE */
Libname INDBASE META  REPNAME='Foundation' LIBURI="SASLibrary?*[@Name='Indicadores Base'][DeployedComponents/ServerContext[@Name='SASApp']]" METAOUT=DATA;

/* Se hace un cruce de info entre BSC_SINI.VALUACION y BSC_SINI.'APERCAB REING'n con REPORTE */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_VALUACION AS 
   SELECT DISTINCT t1.VALUACION, 
          t1.CAUSA, 
          t1.CAUSA_HOMOLOGADA, 
          t2.CVE_AGENTE, 
          t1.Agente, 
          t1.Oficina, 
          t1.'Oficina Beneficiario'n, 
          t1.Gerente, 
          t1.Director, 
          t1.REF, 
          t1.MO, 
          t1.'Importe Reparaciones'n, 
          t1.SUBRAMO, 
          t1.'Fecha Captura'n
      FROM BSC_SINI.VALUACION t1
           LEFT JOIN BSC_SINI.'APERCAB REING'n t2 ON (t1.REPORTE = t2.REPORTE);
QUIT;

/*Se hace cruce entre VALPROD.ANALISTACDR y VALPROD.ASIGNACIONCDR con IDANALISTACDR 
	y que CLAVEANALISTA contenga 'SUPQ'*/
PROC SQL;
   CREATE TABLE WORK.SUPERVISOR_SERVICIO AS 
   SELECT DISTINCT t1.CLAVEANALISTA LABEL='' AS CLAVEANALISTA_SUPSERV, 
          t1.NOMBRENNALISTA LABEL='' AS NOMBRE_SUPERSERV, 
          t2.CLAVETALLER LABEL='' AS CLAVETALLER, 
          t2.CODVALUADOR LABEL='' AS CODVALUADOR, 
          t1.IDREGIONGEOGRAFICA LABEL='' AS IDREGIONGEOGRAFICA
      FROM VALPROD.ANALISTACDR t1
           INNER JOIN VALPROD.ASIGNACIONCDR t2 ON (t1.IDANALISTACDR = t2.IDANALISTACDR)
      WHERE t1.CLAVEANALISTA CONTAINS 'SUPQ'
      ORDER BY t1.CLAVEANALISTA,
               t2.CLAVETALLER;
QUIT;

/* Extracción de BSC_SINI.TCAUSA_BSC cuando la CAUSA_HOMOLOGADA no es DESCONOCIDA */
PROC SQL;
   CREATE TABLE WORK.CODIGO_CAUSA AS 
   SELECT DISTINCT t1.Z_ID, 
          t1.CAUSA, 
          t1.CAUSA_HOMOLOGADA
      FROM BSC_SINI.TCAUSA_BSC t1
      WHERE t1.CAUSA_HOMOLOGADA NOT = 'DESCONOCIDA';
QUIT;

/* Extracción de los distinto en la columna de concepto de VALPROD.COSTO */
PROC SQL;
   CREATE TABLE WORK.Conceptos AS 
   SELECT DISTINCT t1.CONCEPTO
      FROM VALPROD.COSTO t1
      WHERE t1.CONCEPTO NOT IS MISSING;
QUIT;

/* Suma de los montos agrupados por IDEXPEDIENTE y CONCEPTO de VALPROD.COSTO */
PROC SQL;
   CREATE TABLE WORK.BuscaMontosPortal_Inicial AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.CONCEPTO, 
          /* SUM_of_MONTO */
            (SUM(t1.MONTO)) AS SUM_of_MONTO
      FROM VALPROD.COSTO t1
      GROUP BY t1.IDEXPEDIENTE,
               t1.CONCEPTO;
QUIT;

/* Extracción de los distinto en IDEXPEDIENTE, DESCRIPCION de VALPROD.COSTO */
PROC SQL;
   CREATE TABLE WORK.REAL_CE AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          t1.DESCRIPCION
      FROM VALPROD.COSTO t1
      WHERE t1.DESCRIPCION = 'COSTO_POR_MOB_CARRIL_EXP' AND t1.IDEXPEDIENTE NOT IS MISSING;
QUIT;

/* Suma de los montos agrupados por IDEXPEDIENTE y CONCEPTO de VALPROD.COMPLEMENTO */
PROC SQL;
   CREATE TABLE WORK.BuscaMontosPortal_Complemento AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.CONCEPTO, 
          /* SUM_of_MONTO */
            (SUM(t1.MONTO)) AS SUM_of_MONTO
      FROM VALPROD.COMPLEMENTO t1
      GROUP BY t1.IDEXPEDIENTE,
               t1.CONCEPTO;
QUIT;

/* Fecha maxima de FECHAPROMESAREAL y FECHAACTUALIZACION_FPR de VALPROD.FECHAPROMESAREALANLCDR*/
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_FECHAPROMESAREALANLCDR AS 
   SELECT t1.IDEXPEDIENTE, 
          /* MAX_of_FECHAPROMESAREAL */
            (MAX(t1.FECHAPROMESAREAL)) FORMAT=DATETIME27.6 AS MAX_of_FECHAPROMESAREAL, 
          /* MAX_of_FECHAACTUALIZACION_FPR */
            (MAX(t1.FECHAACTUALIZACION_FPR)) FORMAT=DATETIME27.6 AS MAX_of_FECHAACTUALIZACION_FPR
      FROM VALPROD.FECHAPROMESAREALANLCDR t1
      GROUP BY t1.IDEXPEDIENTE
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Extracción de los distinto en la columna de IDEXPEDIENTE de VALPROD.HISTOINVESTIGACION */
PROC SQL;
   CREATE TABLE WORK.ExtraeExpedientesINVEST AS 
   SELECT DISTINCT t1.IDEXPEDIENTE
      FROM VALPROD.HISTOINVESTIGACION t1;
QUIT;

/* Cruce de info entre BSC_SINI.Prestadores y BSC_SINI.tipoProveedor y BSC_SINI.TESTADO_BSC */
PROC SQL;
   CREATE TABLE WORK.Prestadores AS 
   SELECT t1.Id  AS CLAVETALLER, 
          t3.EDOPOB AS Pob_Comer, 
          t1.Marca AS MarcaCDR, 
          t1.Nombre AS NombreCDR, 
          t2.'tipo proveedor'n AS TipoCDR, 
          t1.Nom_Comer AS Nom_CDR_Comer
      FROM BSC_SINI.Prestadores t1
           INNER JOIN BSC_SINI.tipoProveedor t2 ON (t1.Tipo = t2.id)
           LEFT JOIN BSC_SINI.TESTADO_BSC t3 ON (t1.POBCOMER = t3.Z_ID);
QUIT;

/*  Maxima IDHISTORICOTERMINOENTREGA por IDEXPEDIENTE cuando 
	TIPOFECHA este en AUTORIZA VAL. NO REPARACION */
PROC SQL;
   CREATE TABLE WORK.REP_NO_AUT AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          /* MAX_of_IDHISTORICOTERMINOENTREGA */
            (MAX(t1.IDHISTORICOTERMINOENTREGA)) AS MAX_of_IDHISTORICOTERMINOENTREGA
      FROM VALPROD.HISTORICOTERMINOENTREGA t1
      WHERE t1.TIPOFECHA IN 
           (
           'AUTORIZA VAL. NO REPARACION'
           )
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/* Maxima IDHISTORICOTERMINOENTREGA por IDEXPEDIENTE cuando 
	TIPOFECHA este en 'ENVIO COMPLEMENTO TALLER VALUADOR',
           			  'ENVIO COMPLEMENTO ADMINISTRATIVO VALUADOR' */
PROC SQL;
   CREATE TABLE WORK.MAX_ID_EXP AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          /* MAX_of_IDHISTORICOTERMINOENTREGA */
            (MAX(t1.IDHISTORICOTERMINOENTREGA)) AS MAX_of_IDHISTORICOTERMINOENTREGA
      FROM VALPROD.HISTORICOTERMINOENTREGA t1
      WHERE t1.TIPOFECHA IN 
           (
           'ENVIO COMPLEMENTO TALLER VALUADOR',
           'ENVIO COMPLEMENTO ADMINISTRATIVO VALUADOR'
           )
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/*  Minima FECHA por IDEXPEDIENTE cuando 
	TIPOFECHA este en 'ENTREGA', 'ENTREGA UNIDAD'  */
PROC SQL;
   CREATE TABLE WORK.MIN_FEC_ENTREGA AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          /* MIN_of_FECHA */
            (MIN(t1.FECHA)) FORMAT=DATETIME27.6 AS MIN_of_FECHA
      FROM VALPROD.HISTORICOTERMINOENTREGA t1
      WHERE t1.TIPOFECHA IN 
           (
           'ENTREGA',
           'ENTREGA UNIDAD'
           )
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/*  */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_HISTORICOTERMINOENTREG AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          /* MIN_of_FECHA */
            (MIN(t1.FECHA)) FORMAT=DATETIME27.6 AS MIN_of_FECHA, 
          /* COUNT_of_IDHISTORICOTERMINOENTRE */
            (COUNT(t1.IDHISTORICOTERMINOENTREGA)) AS COUNT_of_IDHISTORICOTERMINOENTRE
      FROM VALPROD.HISTORICOTERMINOENTREGA t1
      WHERE t1.TIPOFECHA IN 
           (
           'TERMINO',
           'TERMINO UNIDAD'
           )
      GROUP BY t1.IDEXPEDIENTE
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Conteo mayores a 1 de expedientes dependiendo de TIPOFECHA  */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_HISTORICOTERMINOE_0001 AS 
   SELECT t1.IDEXPEDIENTE, 
          /* COUNT_of_IDEXPEDIENTE */
            (COUNT(t1.IDEXPEDIENTE)) AS COUNT_of_IDEXPEDIENTE
      FROM VALPROD.HISTORICOTERMINOENTREGA t1
      WHERE t1.TIPOFECHA IN 
           (
           'TERMINO UNIDAD',
           'TERMINO'
           )
      GROUP BY t1.IDEXPEDIENTE
      HAVING (CALCULATED COUNT_of_IDEXPEDIENTE) > 1
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Minima fecha por IDEXPEDIENTE cuando TIPOFECHA es 'ENTREGA UNIDAD','ENTREGA' */
PROC SQL;
   CREATE TABLE WORK.MIN_ENTREGA AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          /* MIN_of_FECHA */
            (MIN(t1.FECHA)) FORMAT=DATETIME27.6 AS MIN_of_FECHA, 
          /* COUNT_of_IDHISTORICOTERMINOENTRE */
            (COUNT(t1.IDHISTORICOTERMINOENTREGA)) AS COUNT_of_IDHISTORICOTERMINOENTRE
      FROM VALPROD.HISTORICOTERMINOENTREGA t1
      WHERE t1.TIPOFECHA IN 
           (
           'ENTREGA UNIDAD',
           'ENTREGA'
           )
      GROUP BY t1.IDEXPEDIENTE
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Cruce de información entre VALPROD.TALLERES y  VALPROD.ESTADO, VALPROD.RELACIONCDR_SICDR 
	y VALPROD.SUPERVISORINTEGRAL*/
PROC SQL;
   CREATE TABLE WORK.TALLERES AS 
   SELECT t1.CLAVETALLER, 
          t1.TIPO AS TipoCDR_Portal, 
          t2.NOMBRE AS EstadoCDR, 
          t2.IDREGIONGEOGRAFICA AS RegionValuacion, 
          t4.CLAVESUPERVISOR, 
          t4.NOMBRESUPERVISOR LABEL="NOMBRENNALISTA" AS NOMBRENNALISTA
      FROM VALPROD.TALLERES t1
           LEFT JOIN VALPROD.ESTADO t2 ON (t1.IDESTADO = t2.IDESTADO)
           LEFT JOIN VALPROD.RELACIONCDR_SICDR t3 ON (t1.CLAVETALLER = t3.CLAVETALLER)
           LEFT JOIN VALPROD.SUPERVISORINTEGRAL t4 ON (t3.IDSICDR = t4.IDSUPERVISORINTEGRAL);
QUIT;

/* Identificacion de Transito */
PROC SQL;
   CREATE TABLE WORK.Transito AS 
   SELECT t1.IDEXPEDIENTE, 
          /* Transito */
            (1) AS Transito
      FROM VALPROD.DATOSGENERALES t1, VALPROD.ESTATUS t2, VALPROD.ESTATUSEXPEDIENTES t3, VALPROD.TALLERES t4, 
          VALPROD.ESTADO t5
      WHERE (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND t2.IDESTATUSEXPEDIENTE = t3.IDESTATUSEXPEDIENTE AND t1.CLAVETALLER = 
           t4.CLAVETALLER AND t4.IDESTADO = t5.IDESTADO) AND (t2.TRANSITO = 1 AND t2.PISO = 0 AND 
           t2.IDESTATUSEXPEDIENTE IN 
           (
           '02',
           '03',
           '05',
           '10',
           '04',
           '06',
           '07',
           '09',
           '11',
           '21',
           '22',
           '23',
           '28',
           '30'
           ));
QUIT;

/* Identificar lo pendiente de transito */
PROC SQL;
   CREATE TABLE WORK.PENDIENTE_TRANSITO AS 
   SELECT t1.IDEXPEDIENTE, 
          /* Transito */
            (1) AS Transito
      FROM VALPROD.DATOSGENERALES t1, VALPROD.ESTATUS t2, VALPROD.ESTATUSEXPEDIENTES t3, VALPROD.TALLERES t4, 
          VALPROD.ESTADO t5
      WHERE (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND t2.IDESTATUSEXPEDIENTE = t3.IDESTATUSEXPEDIENTE AND t1.CLAVETALLER = 
           t4.CLAVETALLER AND t4.IDESTADO = t5.IDESTADO) AND (t2.TRANSITO = 1 AND t2.PISO = 0 AND 
           t2.IDESTATUSEXPEDIENTE IN 
           (
           '32'
           ));
QUIT;

/* Identificación de Piso */
PROC SQL;
   CREATE TABLE WORK.Piso AS 
   SELECT t1.IDEXPEDIENTE, 
          /* Piso */
            (1) AS Piso
      FROM VALPROD.DATOSGENERALES t1, VALPROD.ESTATUS t2, VALPROD.ESTATUSEXPEDIENTES t3, VALPROD.TALLERES t4, 
          VALPROD.ESTADO t5
      WHERE (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND t2.IDESTATUSEXPEDIENTE = t3.IDESTATUSEXPEDIENTE AND t1.CLAVETALLER = 
           t4.CLAVETALLER AND t4.IDESTADO = t5.IDESTADO) AND (t2.IDESTATUSEXPEDIENTE IN 
           (
           '02',
           '03',
           '05',
           '08',
           '10',
           '04',
           '06',
           '07',
           '09',
           '11',
           '21',
           '23',
           '28'
           ) AND t2.PISO = 1);
QUIT;

/* Cruce de información entre VALPROD.DATOSGENERALES y VALPROD.TALLERES, VALPROD.VALE y
	VALPROD.VALEHISTORICO cuando el ejercicio sea mayr o igual al 21*/
PROC SQL;
   CREATE TABLE WORK.PZA AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.IDVALUACION, 
          t1.CLAVETALLER, 
          t2.COTIZADOR AS CDRCOTIZADOR, 
          t2.FRONTERIZO AS CDRAUTOSURTIDO, 
          t3.FECHAEXPEDICION, 
          t3.FECHAACTUALIZACION, 
          t3.IDVALEESTATUS, 
          t4.FECENVIO, 
          t4.FECENTREGAREFACCIONARIA, 
          t4.FECRECEPCION, 
          t4.NUMPARTE, 
          t4.REFERENCIA, 
          t4.DESCRIPCION, 
          /* LLAVEPIEZA */
            (cat(t1.IDEXPEDIENTE,trim(t4.REFERENCIA),trim(t4.NUMPARTE),trim(t4.DESCRIPCION))) AS LLAVEPIEZA
      FROM VALPROD.DATOSGENERALES t1
           LEFT JOIN VALPROD.TALLERES t2 ON (t1.CLAVETALLER = t2.CLAVETALLER)
           INNER JOIN VALPROD.VALE t3 ON (t1.IDEXPEDIENTE = t3.IDEXPEDIENTE)
           INNER JOIN VALPROD.VALEHISTORICO t4 ON (t3.IDVALE = t4.IDVALE)
      WHERE t1.EJERCICIO >= '21';
QUIT;

/* Inner entre VALPROD.DATOSGENERALES y VALPROD.ESTATUS */
PROC SQL;
   CREATE TABLE WORK.Asignado AS 
   SELECT t1.IDEXPEDIENTE, 
          /* Asignado */
            (1) AS Asignado
      FROM VALPROD.DATOSGENERALES t1
           INNER JOIN VALPROD.ESTATUS t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE)
      WHERE t2.IDESTATUSEXPEDIENTE = '01';
QUIT;

/* Cruce entre VALPROD.DATOSVEHICULO y VALPROD.COLOR, VALPROD.UNIDAD y VALPROD.MARCA*/
PROC SQL;
   CREATE TABLE WORK.DATOSVEHICULO AS 
   SELECT t1.IDEXPEDIENTE, 
          t2.DESCRIPCION AS Color, 
          t3.DESCRIPCION AS UNIDAD, 
          t4.DESCRIPCION AS 'Marca Vehiculo'n, 
          t1.TIPO, 
          t1.MODELO, 
          t1.PLACAS, 
          t1.SERIE
      FROM VALPROD.DATOSVEHICULO t1
           LEFT JOIN VALPROD.COLOR t2 ON (t1.IDCOLOR = t2.IDCOLOR)
           LEFT JOIN VALPROD.UNIDAD t3 ON (t1.IDUNIDAD = t3.IDUNIDAD)
           LEFT JOIN VALPROD.MARCA t4 ON (t1.IDMARCA = t4.IDMARCA);
QUIT;

/* minimo de FECHAENVIOTALLER  de VALPROD.ENVIOHISTORICO */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_ENVIOHISTORICO AS 
   SELECT t1.IDEXPEDIENTE, 
          /* MIN_of_FECHAENVIOTALLER */
            (MIN(t1.FECHAENVIOTALLER)) FORMAT=DATETIME27.6 AS MIN_of_FECHAENVIOTALLER
      FROM VALPROD.ENVIOHISTORICO t1
      WHERE t1.FECHAENVIOTALLER NOT IS MISSING AND t1.IDEXPEDIENTE NOT = .
      GROUP BY t1.IDEXPEDIENTE
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* minimo de FECHAAUTORIZACIONVALUADOR  de VALPROD.ENVIOHISTORICO */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_ENVIOHISTORICO_0000 AS 
   SELECT t1.IDEXPEDIENTE, 
          /* MIN_of_FECHAAUTORIZACIONVALUADOR */
            (MIN(t1.FECHAAUTORIZACIONVALUADOR)) FORMAT=DATETIME27.6 AS MIN_of_FECHAAUTORIZACIONVALUADOR
      FROM VALPROD.ENVIOHISTORICO t1
      WHERE t1.IDEXPEDIENTE NOT = .
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/* Conteo del minimo de FECHAAUTORIZACIONVALUADOR VALPROD.ENVIOHISTORICO
	cuando el IDEXPEDIENTE no es null */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_ENVIOHISTORICO_0001 AS 
   SELECT t1.IDEXPEDIENTE, 
          /* COUNT DISTINCT_of_FECHAAUTORIZAC */
            (COUNT(DISTINCT(t1.FECHAAUTORIZACIONVALUADOR))) AS 'COUNT DISTINCT_of_FECHAAUTORIZAC'n
      FROM VALPROD.ENVIOHISTORICO t1
      WHERE t1.IDEXPEDIENTE NOT = .
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/* Maximo FECHAENVIOTALLER de VALPROD.ENVIOHISTORICO donde FECHAENVIOTALLER no es missing 
	y IDEXPEDIENTE no es vacio */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_ENVIOHISTORICO_0002 AS 
   SELECT t1.IDEXPEDIENTE, 
          /* MAX_of_FECHAENVIOTALLER */
            (MAX(t1.FECHAENVIOTALLER)) FORMAT=DATETIME27.6 AS MAX_of_FECHAENVIOTALLER
      FROM VALPROD.ENVIOHISTORICO t1
      WHERE t1.FECHAENVIOTALLER NOT IS MISSING AND t1.IDEXPEDIENTE NOT = .
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/* Maximo FECHAAUTORIZACIONVALUADOR de VALPROD.ENVIOHISTORICO donde FECHAAUTORIZACIONVALUADOR
	no es missing y IDEXPEDIENTE no es vacio*/
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_ENVIOHISTORICO_0003 AS 
   SELECT t1.IDEXPEDIENTE, 
          /* MAX_of_FECHAAUTORIZACIONVALUADOR */
            (MAX(t1.FECHAAUTORIZACIONVALUADOR)) FORMAT=DATETIME27.6 AS MAX_of_FECHAAUTORIZACIONVALUADOR
      FROM VALPROD.ENVIOHISTORICO t1
      WHERE t1.FECHAAUTORIZACIONVALUADOR NOT IS MISSING AND t1.IDEXPEDIENTE NOT = .
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/* Se crea la diferencia entre fechas para crear HORAS_VALUACION, HORAS_VALUADOR, HORAS_CARRUSEL
	y la creación de 'tipoDesvío'n*/
PROC SQL;
   CREATE TABLE WORK.SeleccionaExpedientes AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.FECHAENVIOTALLER, 
          t1.FECHAASIGNACIONVALUADOR, 
          t1.FECHAAUTORIZACIONVALUADOR, 
          /* HORAS_VALUACION */
            (INTCK('DTMINUTE',t1.FECHAENVIOTALLER,t1.FECHAAUTORIZACIONVALUADOR)/60) FORMAT=6.1 AS HORAS_VALUACION, 
          /* HORAS_VALUADOR */
            (INTCK('DTMINUTE',t1.FECHAASIGNACIONVALUADOR,t1.FECHAAUTORIZACIONVALUADOR)/60) FORMAT=6.1 AS HORAS_VALUADOR, 
          /* HORAS_CARRUSEL */
            (INTCK('DTMINUTE',t1.FECHAENVIOTALLER,t1.FECHAASIGNACIONVALUADOR)/60) FORMAT=6.1  AS HORAS_CARRUSEL, 
          /* expValido */
            (case when
            (INTCK('DTMINUTE',t1.FECHAENVIOTALLER,t1.FECHAAUTORIZACIONVALUADOR)/60) is missing or (INTCK('DTsecond'
            ,t1.FECHAENVIOTALLER,t1.FECHAAUTORIZACIONVALUADOR)/60)<-300
            or
            (INTCK('DTMINUTE',t1.FECHAASIGNACIONVALUADOR,t1.FECHAAUTORIZACIONVALUADOR)/60) is missing or (INTCK(
            'DTsecond',t1.FECHAASIGNACIONVALUADOR,t1.FECHAAUTORIZACIONVALUADOR)/60)<-300
            or
            (INTCK('DTMINUTE',t1.FECHAENVIOTALLER,t1.FECHAASIGNACIONVALUADOR)/60) is missing
            then
            0 else 1
            end) AS expValido, 
          /* tipoDesvío */
            (case when INTCK('DTMINUTE',t1.FECHAENVIOTALLER,t1.FECHAAUTORIZACIONVALUADOR)/60 is missing or INTCK(
            'DTsecond',t1.FECHAENVIOTALLER,t1.FECHAAUTORIZACIONVALUADOR)/60 <- 300 then 'EnvíoAutorizaciónInválido'
            when INTCK('DTMINUTE',t1.FECHAASIGNACIONVALUADOR,t1.FECHAAUTORIZACIONVALUADOR)/60 is missing or INTCK(
            'DTsecond',t1.FECHAASIGNACIONVALUADOR,t1.FECHAAUTORIZACIONVALUADOR)/60 <- 300 then 
            'CarruselAutorizaciónInválido'
            when INTCK('DTMINUTE',t1.FECHAENVIOTALLER,t1.FECHAASIGNACIONVALUADOR)/60 is missing then 
            'EnvíoCarruselInválido'
            else ''
            end) AS 'tipoDesvío'n
      FROM VALPROD.ENVIOHISTORICO t1
      WHERE t1.IDEXPEDIENTE NOT IS MISSING
      ORDER BY t1.IDEXPEDIENTE,
               t1.FECHAENVIOTALLER;
QUIT;

/* Unicos IDEXPEDIENTE de WORK.SELECCIONAEXPEDIENTES cuando expValido=0 */
PROC SQL;
   CREATE TABLE WORK.SeparaDesviaciones AS 
   SELECT DISTINCT t1.IDEXPEDIENTE
      FROM WORK.SELECCIONAEXPEDIENTES t1
      WHERE t1.expValido = 0;
QUIT;

/* Se hace un cruce entre WORK.SELECCIONAEXPEDIENTES y WORK.SEPARADESVIACIONES por IDEXPEDIENTE
	y se hace una bandera 'ExpedienteVálidoTiempos'n*/
PROC SQL;
   CREATE TABLE WORK.Envios AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.FECHAENVIOTALLER, 
          /* ExpedienteVálidoTiempos */
            (IFN(t1.IDEXPEDIENTE<>t2.IDEXPEDIENTE,1,0)) AS 'ExpedienteVálidoTiempos'n
      FROM WORK.SELECCIONAEXPEDIENTES t1
           LEFT JOIN WORK.SEPARADESVIACIONES t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE);
QUIT;

/* Se hace un cruce entre WORK.SELECCIONAEXPEDIENTES y WORK.SEPARADESVIACIONES por IDEXPEDIENTE */
PROC SQL;
   CREATE TABLE WORK.Autorizaciones AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.FECHAAUTORIZACIONVALUADOR
      FROM WORK.SELECCIONAEXPEDIENTES t1
           LEFT JOIN WORK.SEPARADESVIACIONES t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE);
QUIT;

/* Manipulacion base tiempos */
data work.EnumeraEnvios;
set WORK.envios;
CT+1;
by idexpediente;
if first.idexpediente then ct=0;
run;
data work.EnumeraCarrusel;
set work.carrusel;
ct+1;
by idexpediente;
if first.idexpediente then ct=1;
run;

data work.EnumeraEnvios_1;
set work.envios;
CT+1;
by idexpediente;
if first.idexpediente then ct=1;
run;

data work.EnumeraAutorizaciones;
set work.autorizaciones;
ct+1;
by idexpediente;
if first.idexpediente then ct=1;
run;

/* Inner join entre WORK.ENUMERAAUTORIZACIONES y WORK.ENUMERAENVIOS */
PROC SQL;
   CREATE TABLE WORK.EvaluaTiempoTaller AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.FECHAAUTORIZACIONVALUADOR AS '(i-1)autorizacionVal'n, 
          t2.FECHAENVIOTALLER, 
          /* HORAS_CDR */
            (intck('dtminute',t1.FECHAAUTORIZACIONVALUADOR,t2.FECHAENVIOTALLER)/60) FORMAT=6.1 AS HORAS_CDR, 
          /* expValido */
            (ifn((intck('dtminute',t1.FECHAAUTORIZACIONVALUADOR,t2.FECHAENVIOTALLER)/60)<-.1,0,1)) AS expValido
      FROM WORK.ENUMERAAUTORIZACIONES t1
           INNER JOIN WORK.ENUMERAENVIOS t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND (t1.ct = t2.CT));
QUIT;

/* Inner join entre WORK.ENUMERAENVIOS_1 y WORK.ENUMERACARRUSEL */
PROC SQL;
   CREATE TABLE WORK.TiempoPrimerCarrusel AS 
   SELECT t1.IDEXPEDIENTE, 
          t2.FECHAASIGNACIONVALUADOR, 
          /* TiempoPrimerCarrusel */
            (intck('dtminute',t1.FECHAENVIOTALLER,t2.FECHAASIGNACIONVALUADOR)/60) FORMAT=5.3 AS TiempoPrimerCarrusel
      FROM WORK.ENUMERAENVIOS_1 t1
           INNER JOIN WORK.ENUMERACARRUSEL t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND (t1.CT = t2.ct))
      WHERE t1.CT = 1
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Inner entre WORK.ENUMERAENVIOS_1 y WORK.ENUMERACARRUSEL 
	y creación de TiempoCarruselComplementos */
PROC SQL;
   CREATE TABLE WORK.TiempoCarruselComplementos AS 
   SELECT t1.IDEXPEDIENTE, 
          /* TiempoCarruselComplementos */
            (intck('dtminute',t1.FECHAENVIOTALLER,t2.FECHAASIGNACIONVALUADOR)/60) FORMAT=5.3 AS TiempoCarruselComplementos
      FROM WORK.ENUMERAENVIOS_1 t1
           INNER JOIN WORK.ENUMERACARRUSEL t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND (t1.CT = t2.ct))
      WHERE t1.CT > 1
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Inner entre WORK.ENUMERAAUTORIZACIONES y WORK.ENUMERAENVIOS_1 y 
	creacion de 'TiempoAutorizaciónComplementos'n */
PROC SQL;
   CREATE TABLE WORK.TiempoValuacionComplementos AS 
   SELECT t1.IDEXPEDIENTE, 
          /* TiempoAutorizaciónComplementos */
            (sum(intck('dtminute',t2.FECHAENVIOTALLER,t1.FECHAAUTORIZACIONVALUADOR)/60)) FORMAT=6.1  AS 'TiempoAutorizaciónComplementos'n
      FROM WORK.ENUMERAAUTORIZACIONES t1
           INNER JOIN WORK.ENUMERAENVIOS_1 t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND (t1.ct = t2.CT))
      WHERE t1.ct > 1
      GROUP BY t1.IDEXPEDIENTE
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Inner entre WORK.ENUMERAAUTORIZACIONES y WORK.ENUMERAENVIOS_1 
	y creacion de TiempoPrimeraAutorizacion */
PROC SQL;
   CREATE TABLE WORK.TiempoPrimeraValuacion AS 
   SELECT t1.IDEXPEDIENTE, 
          /* TiempoPrimeraAutorizacion */
            (intck('dtminute',t2.FECHAENVIOTALLER,t1.FECHAAUTORIZACIONVALUADOR)/60) FORMAT=6.1 AS TiempoPrimeraAutorizacion
      FROM WORK.ENUMERAAUTORIZACIONES t1
           INNER JOIN WORK.ENUMERAENVIOS_1 t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE AND (t1.ct = t2.CT))
      WHERE t1.ct = 1
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Maximo de envios por expediente */
PROC SQL;
   CREATE TABLE WORK.CuentaEnvios AS 
   SELECT t1.IDEXPEDIENTE, 
          /* ENVIOS */
            (MAX(t1.CT)) AS ENVIOS
      FROM WORK.ENUMERAENVIOS_1 t1
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/* IDEXPEDIENTE unicos cuando expValido = 0 */
PROC SQL;
   CREATE TABLE WORK.IdentificaDesviaciones AS 
   SELECT DISTINCT t1.IDEXPEDIENTE
      FROM WORK.EVALUATIEMPOTALLER t1
      WHERE t1.expValido = 0;
QUIT;

/* Suma de TiempoCarruselComplementos por IDEXPEDIENTE */
PROC SQL;
   CREATE TABLE WORK.ConsolidaTiemposCarruselComp AS 
   SELECT t1.IDEXPEDIENTE, 
          /* tiempoCarrComp */
            (sum(t1.TiempoCarruselComplementos)) FORMAT=6.3 AS tiempoCarrComp
      FROM WORK.TIEMPOCARRUSELCOMPLEMENTOS t1
      GROUP BY t1.IDEXPEDIENTE;
QUIT;

/* Cruce entre WORK.TIEMPOPRIMERAVALUACION y WORK.TIEMPOVALUACIONCOMPLEMENTOS
	y suma de TiempoPrimeraAutorizacion y TiempoAutorizaciónComplementos */
PROC SQL;
   CREATE TABLE WORK.TiempoTotalValuacion AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.TiempoPrimeraAutorizacion, 
          t3.'TiempoAutorizaciónComplementos'n, 
          /* tiempoTotalAutorización */
            (sum(t1.TiempoPrimeraAutorizacion,t3.'TiempoAutorizaciónComplementos'n)) FORMAT=6.1 AS 'tiempoTotalAutorización'n
      FROM WORK.TIEMPOPRIMERAVALUACION t1
           LEFT JOIN WORK.TIEMPOVALUACIONCOMPLEMENTOS t3 ON (t1.IDEXPEDIENTE = t3.IDEXPEDIENTE);
QUIT;

/* Creacion de horasTallerComplementos */
PROC SQL;
   CREATE TABLE WORK.TiempoTallerComplementos AS 
   SELECT t2.IDEXPEDIENTE, 
          /* horasTallerComplementos */
            (sum(t2.horas_cdr)) FORMAT=6.1 AS horasTallerComplementos, 
          /* expValido */
            (ifn(t1.IDEXPEDIENTE=t2.IDEXPEDIENTE,0,1)) AS expValido
      FROM WORK.IDENTIFICADESVIACIONES t1
           RIGHT JOIN WORK.EVALUATIEMPOTALLER t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE)
      GROUP BY t2.IDEXPEDIENTE,
               (CALCULATED expValido);
QUIT;

/* Creación de tiempoTotalCarrusel */
PROC SQL;
   CREATE TABLE WORK.TiempoTotalCarrusel AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.FECHAASIGNACIONVALUADOR AS FEC_PRIMERA_ASIG_CARR, 
          t1.TiempoPrimerCarrusel, 
          t2.tiempoCarrComp, 
          /* tiempoTotalCarrusel */
            (sum(t1.TiempoPrimerCarrusel,t2.tiempoCarrComp)) FORMAT=6.3 AS tiempoTotalCarrusel
      FROM WORK.TIEMPOPRIMERCARRUSEL t1
           LEFT JOIN WORK.CONSOLIDATIEMPOSCARRUSELCOMP t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE);
QUIT;

/* Minimo en FECHAENVIOTALLER por IDEXPEDIENTE y ExpedienteVálidoTiempos */
PROC SQL;
   CREATE TABLE WORK.MinimoEnvioCDR AS 
   SELECT t1.IDEXPEDIENTE, 
          /* MIN_of_FECHAENVIOTALLER */
            (MIN(t1.FECHAENVIOTALLER)) FORMAT=DATETIME27.6 AS MIN_of_FECHAENVIOTALLER, 
          t1.'ExpedienteVálidoTiempos'n
      FROM WORK.ENVIOS t1
      GROUP BY t1.IDEXPEDIENTE,
               t1.'ExpedienteVálidoTiempos'n;
QUIT;

/*  */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_DATOSGENERALES AS 
   SELECT t1.IDEXPEDIENTE AS idExp, 
          /* VALUACION */
            (SUBSTR(t26.NUMEXPEDIENTE,1,14)) AS VALUACION, 
          t1.EJERCICIO AS Ejercicio, 
          t1.NUMREPORTE AS Reporte, 
          t1.NUMSINIESTRO AS Siniestro, 
          t1.CODAFECTADO, 
          t1.CODIGOASEGURADO, 
          t1.TIPORIESGO, 
          t1.NUMPOLIZA, 
          t1.NUMENDOSO, 
          t1.NUMINCISO, 
          t28.'Marca Vehiculo'n AS MarcaVehiculo, 
          t28.TIPO, 
          t28.MODELO, 
          t28.Color, 
          t28.PLACAS, 
          t28.SERIE, 
          t28.UNIDAD, 
          t1.NOMCONDUCTOR, 
          /* CLIENTE */
            (' ') AS CLIENTE, 
          /* OFICINA */
            (' ') AS OFICINA, 
          /* GRUPO/NEGOCIO */
            (' ') AS 'GRUPO/NEGOCIO'n, 
          t1.PRESUPUESTOMOB, 
          t1.PIEZASCAMBIO, 
          t1.SUMAASEG AS SUMAASEG_DG, 
          t1.MONTODEDUCIBLE AS MONTODEDUCIBLE_DG, 
          t4.TRANSITO, 
          t4.PISO, 
          /* Transito_Activo */
            (IFN(t4.IDESTATUSEXPEDIENTE IN ('02', '03', '05', '10', '04','06', '07', '09', '11', '21', '22' ,  '28', 
            '32', '30') AND t4.PISO = 0 AND t4.TRANSITO = 1,1,0)) AS Transito_Activo, 
          /* Piso_Activo */
            (IFN(t4.IDESTATUSEXPEDIENTE IN ('02', '03', '05', '08', '10', '04', '06', '07', '09', '11', '21', '22', '23'
            , '28', '31') AND t4.PISO = 1,1,0)) AS Piso_Activo, 
          t4.GRUA, 
          t1.CLAVETALLER, 
          t12.Pob_Comer, 
          t12.MarcaCDR, 
          t12.NombreCDR, 
          t12.Nom_CDR_Comer, 
          t12.TipoCDR, 
          t1.CODVALUADOR, 
          t16.NOMBRE AS CatValuador, 
          t1.ORIGEN AS HerramientaValuacion, 
          t8.DESCRIPCION AS EstatusValuacion, 
          /* TipoValuador */
            (ifc(t1.codvaluador not is missing,ifc(t2.equipopesado = 1,'EP','AUTOS'),'SIN VALUADOR')) AS TipoValuador, 
          /* Bandeja */
            (case when t5.idestatusexpediente in ('02','03','05','08','10','04','06','07','09','11','16','17','21','22',
            '23','28','30') and t4.piso = 1 then 'PISO'
                     when t5.idestatusexpediente in ('02','03','05','08','10','04','06','07','09','11','16','17','28',
            '30') and t4.transito = 1 and t4.piso = 0 then 'TRANSITO'
            else 'SIN BANDEJA'
            end) AS Bandeja, 
          t3.FECASIGNACION, 
          t3.FECADJUDICACION, 
          /* FECENVIO */
            (t9.MIN_of_FECHAENVIOTALLER) FORMAT=DATETIME16. AS FECENVIO, 
          t3.FECVALUACION, 
          t3.FECINGRESO, 
          t21.MIN_of_FECHA AS PRIMERTERMINO, 
          t13.MIN_of_FECHA AS PRIMERENTREGA, 
          t3.FECTERMINADO, 
          t3.FECENTREGADO, 
          t3.FECPROMESA, 
          t3.FECHAESTENT, 
          /* DIFDIASTERMINO */
            (intck('dtweekday1w',t21.MIN_of_FECHA,t3.FECTERMINADO)) AS DIFDIASTERMINO, 
          t3.DIASREPARACION AS DiasReparacion, 
          t5.DESCRIPCION AS EstatusExpediente, 
          /* DiasEnv */
            (case when t9.min_of_fechaenviotaller not is missing
            then intck('DTWEEKDAY',t3.fecadjudicacion,t9.MIN_of_FECHAENVIOTALLER)
            else intck('DTWEEKDAY',t3.fecadjudicacion,datetime())
            end) FORMAT=7.3 AS DiasEnv, 
          t27.RegionValuacion, 
          t27.EstadoCDR, 
          t27.CLAVESUPERVISOR, 
          t27.NOMBRENNALISTA, 
          t27.TipoCDR_Portal, 
          /* Complemento */
            (ifn(t17.'COUNT DISTINCT_of_FECHAAUTORIZAC'n>1,1,0)) AS Complemento, 
          /* UltimoEnvioCDR */
            (t18.MAX_of_FECHAENVIOTALLER) FORMAT=DATETIME16. AS UltimoEnvioCDR, 
          /* PrimeraAutorizacion */
            (t14.MIN_of_FECHAAUTORIZACIONVALUADOR) AS PrimeraAutorizacion, 
          /* UltimaAutorizacion */
            (t19.MAX_of_FECHAAUTORIZACIONVALUADOR) FORMAT=DATETIME16. AS UltimaAutorizacion, 
          /* DiasDifEnvioAutorizacion */
            (intck('dtweekday',t18.MAX_of_FECHAENVIOTALLER,t19.MAX_of_FECHAAUTORIZACIONVALUADOR)) FORMAT=BEST12. AS DiasDifEnvioAutorizacion, 
          /* FecAutMayor */
            (ifn(t19.MAX_of_FECHAAUTORIZACIONVALUADOR>t18.MAX_of_FECHAENVIOTALLER,1,0)) FORMAT=BEST12. AS FecAutMayor, 
          /* CAMBIOTERMINO */
            (ifn(t1.IDEXPEDIENTE=t22.IDEXPEDIENTE,1,0)) AS CAMBIOTERMINO, 
          /* VehEntregado */
            (IFN(t3.FECENTREGADO NOT IS MISSING,1,0)) AS VehEntregado, 
          /* TablaSupervisor */
            (IFN(MONTH(TODAY()) = 1,
                 IFN(YEAR(DATEPART(t3.FECTERMINADO)) = YEAR(TODAY()) - 1 AND MONTH(DATEPART(t3.FECTERMINADO)) = 11,1,0),
                      IFN(MONTH(TODAY()) = 2,
                           IFN(YEAR(DATEPART(t3.FECTERMINADO)) = YEAR(TODAY()) - 1 AND MONTH(DATEPART(t3.FECTERMINADO)) 
            = 12,1,0),
                           IFN(YEAR(DATEPART(t3.FECTERMINADO)) = YEAR(TODAY()) AND MONTH(DATEPART(t3.FECTERMINADO)) = 
            MONTH(TODAY()) - 2,1,0),
                  0),
            0)) AS TablaSupervisor, 
          /* SinSiniestro */
            (IFN(t4.HISTOSINSINIESTRO NOT IS MISSING,1,0)) LABEL="SinSiniestro" AS SinSiniestro, 
          t1.AGENTE, 
          t1.OFIEMICODIGO, 
          t1.OFIEMIDESCRIP, 
          t6.Z_ID AS 'CAUSACODIGO 'n, 
          t6.CAUSA LABEL='' AS CAUSADESCRIP, 
          t1.SUBRAMO
      FROM VALPROD.DATOSGENERALES t1
           LEFT JOIN VALPROD.VALUADOR t2 ON (t1.CODVALUADOR = t2.CODVALUADOR)
           LEFT JOIN VALPROD.FECHAS t3 ON (t1.IDEXPEDIENTE = t3.IDEXPEDIENTE)
           LEFT JOIN VALPROD.ESTATUS t4 ON (t1.IDEXPEDIENTE = t4.IDEXPEDIENTE)
           LEFT JOIN VALPROD.ESTATUSEXPEDIENTES t5 ON (t4.IDESTATUSEXPEDIENTE = t5.IDESTATUSEXPEDIENTE)
           LEFT JOIN VALPROD.VALUACION t8 ON (t1.IDVALUACION = t8.IDVALUACION)
           LEFT JOIN WORK.QUERY_FOR_ENVIOHISTORICO t9 ON (t1.IDEXPEDIENTE = t9.IDEXPEDIENTE)
           LEFT JOIN WORK.QUERY_FOR_ENVIOHISTORICO_0000 t14 ON (t1.IDEXPEDIENTE = t14.IDEXPEDIENTE)
           LEFT JOIN VALPROD.CATEGORIA t16 ON (t2.IDCATEGORIA = t16.IDCATEGORIA)
           LEFT JOIN WORK.QUERY_FOR_ENVIOHISTORICO_0001 t17 ON (t1.IDEXPEDIENTE = t17.IDEXPEDIENTE)
           LEFT JOIN WORK.QUERY_FOR_ENVIOHISTORICO_0002 t18 ON (t1.IDEXPEDIENTE = t18.IDEXPEDIENTE)
           LEFT JOIN WORK.QUERY_FOR_ENVIOHISTORICO_0003 t19 ON (t1.IDEXPEDIENTE = t19.IDEXPEDIENTE)
           LEFT JOIN WORK.QUERY_FOR_HISTORICOTERMINOENTREG t21 ON (t1.IDEXPEDIENTE = t21.IDEXPEDIENTE)
           LEFT JOIN WORK.QUERY_FOR_HISTORICOTERMINOE_0001 t22 ON (t1.IDEXPEDIENTE = t22.IDEXPEDIENTE)
           LEFT JOIN VALPROD.EXPEDIENTE t26 ON (t1.IDEXPEDIENTE = t26.IDEXPEDIENTE)
           LEFT JOIN WORK.PRESTADORES t12 ON (t1.CLAVETALLER = t12.CLAVETALLER)
           LEFT JOIN WORK.MIN_ENTREGA t13 ON (t1.IDEXPEDIENTE = t13.IDEXPEDIENTE)
           LEFT JOIN WORK.TALLERES t27 ON (t1.CLAVETALLER = t27.CLAVETALLER)
           LEFT JOIN WORK.DATOSVEHICULO t28 ON (t1.IDEXPEDIENTE = t28.IDEXPEDIENTE)
           LEFT JOIN WORK.CODIGO_CAUSA t6 ON (t1.CAUSACODIGO = t6.Z_ID)
      WHERE t1.IDEXPEDIENTE NOT IS MISSING;
QUIT;

/* Union de expediente y el concepto */
PROC SQL;
   CREATE TABLE WORK.EXPEDIENTES AS 
   SELECT t1.IDEXPEDIENTE, 
          t2.CONCEPTO
      FROM VALPROD.EXPEDIENTE t1,WORK.CONCEPTOS t2
      ORDER BY t1.IDEXPEDIENTE;
QUIT;

/* Union entre WORK.BUSCAMONTOSPORTAL_COMPLEMENTO y WORK.BUSCAMONTOSPORTAL_INICIAL 
	y WORK.EXPEDIENTES */
PROC SQL;
   CREATE TABLE WORK.ConsolidaMontosPortal AS 
   SELECT t3.IDEXPEDIENTE, 
          t3.CONCEPTO, 
          /* MONTO */
            (case
            when t1.SUM_of_MONTO is missing and t2.SUM_of_MONTO is missing
            then 0
            else case
            when t1.SUM_of_MONTO is missing and t2.SUM_of_MONTO not is missing
            then t2.SUM_of_MONTO
            else case
            when t1.SUM_of_MONTO not is missing and t2.SUM_of_MONTO is missing
            then t1.SUM_of_MONTO
            else t1.SUM_of_MONTO+t2.SUM_of_MONTO
            end
            end
            end) LABEL="MONTO" AS MONTO
      FROM WORK.BUSCAMONTOSPORTAL_COMPLEMENTO t2
           RIGHT JOIN (WORK.BUSCAMONTOSPORTAL_INICIAL t1
           RIGHT JOIN WORK.EXPEDIENTES t3 ON (t1.IDEXPEDIENTE = t3.IDEXPEDIENTE) AND (t1.CONCEPTO = t3.CONCEPTO)) ON 
          (t2.IDEXPEDIENTE = t3.IDEXPEDIENTE) AND (t2.CONCEPTO = t3.CONCEPTO);
QUIT;

/* Transponer */
PROC SQL;
	CREATE VIEW WORK.SORTTempTableSorted AS
		SELECT T.MONTO, T.CONCEPTO, T.IDEXPEDIENTE
	FROM WORK.CONSOLIDAMONTOSPORTAL as T
;
QUIT;

PROC TRANSPOSE DATA=WORK.SORTTempTableSorted
	OUT=WORK.TRNSTRANSPOSED(LABEL="Transposed WORK.CONSOLIDAMONTOSPORTAL")
	NAME=Source
	LABEL=Label;
	BY IDEXPEDIENTE;
	ID CONCEPTO;
	VAR MONTO;
RUN; QUIT;

PROC SQL;
   CREATE TABLE WORK.CONSOLIDAMONTOS AS 
   SELECT t1.IDEXPEDIENTE, 
          /* MO */
            (IFN(t1.HYP IS MISSING,t1.MOB,IFN(t1.MOB IS MISSING,t1.HYP,IFN(t1.HYP NOT IS MISSING AND t1.MOB NOT IS 
            MISSING,t1.HYP+t1.MOB,0)))) LABEL="MO" AS MO, 
          /* REF */
            (IFN(t1.REF IS MISSING,0,t1.REF)) LABEL="REF" AS REF
      FROM WORK.TRNSTRANSPOSED t1;
QUIT;

/* Union entre WORK.REP_NO_AUT y VALPROD.HISTORICOTERMINOENTREGA  */
PROC SQL;
   CREATE TABLE WORK.FEC_AUT_NO_REP AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          t2.FECHA AS FECHA_NO_AUT
      FROM WORK.REP_NO_AUT t1
           LEFT JOIN VALPROD.HISTORICOTERMINOENTREGA t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE) AND 
          (t1.MAX_of_IDHISTORICOTERMINOENTREGA = t2.IDHISTORICOTERMINOENTREGA);
QUIT;

PROC SQL;
   CREATE TABLE WORK.MAX_FEC_EXP AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          t2.FECHA
      FROM WORK.MAX_ID_EXP t1
           LEFT JOIN VALPROD.HISTORICOTERMINOENTREGA t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE) AND 
          (t1.MAX_of_IDHISTORICOTERMINOENTREGA = t2.IDHISTORICOTERMINOENTREGA);
QUIT;

/* Union entre WORK.MAX_FEC_EXP y WORK.MIN_FEC_ENTREGA y WORK.FEC_AUT_NO_REP  */
PROC SQL;
   CREATE TABLE WORK.REINGRESO AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          t1.FECHA, 
          t2.MIN_of_FECHA, 
          t3.FECHA_NO_AUT, 
          /* Reingreso_Portal */
            (case
            when t2.MIN_of_FECHA is not missing and ((t1.FECHA-t2.MIN_of_FECHA)/86400)>1 and t1.FECHA>t3.FECHA_NO_AUT
            then 1
            else 0
            end) AS Reingreso_Portal
      FROM WORK.MAX_FEC_EXP t1
           LEFT JOIN WORK.MIN_FEC_ENTREGA t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE)
           LEFT JOIN WORK.FEC_AUT_NO_REP t3 ON (t1.IDEXPEDIENTE = t3.IDEXPEDIENTE);
QUIT;

PROC SQL;
   CREATE TABLE WORK.Bandejas AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.CLAVETALLER, 
          t2.Piso, 
          t3.Transito, 
          t4.Asignado, 
          t5.Transito AS Pendiente_Transito
      FROM VALPROD.DATOSGENERALES t1
           LEFT JOIN WORK.PISO t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE)
           LEFT JOIN WORK.TRANSITO t3 ON (t1.IDEXPEDIENTE = t3.IDEXPEDIENTE)
           LEFT JOIN WORK.ASIGNADO t4 ON (t1.IDEXPEDIENTE = t4.IDEXPEDIENTE)
           LEFT JOIN WORK.PENDIENTE_TRANSITO t5 ON (t1.IDEXPEDIENTE = t5.IDEXPEDIENTE);
QUIT;

PROC SQL;
   CREATE TABLE WORK.bandeja AS 
   SELECT t1.CLAVETALLER, 
          /* Asignados */
            (SUM(t1.Asignado)) AS Asignados, 
          /* Transitos */
            (SUM(t1.Transito)) AS Transitos, 
          /* Pisos */
            (SUM(t1.Piso)) AS Pisos, 
          /* Pendiente_Transito */
            (SUM(t1.Pendiente_Transito)) AS Pendiente_Transito
      FROM WORK.BANDEJAS t1
      GROUP BY t1.CLAVETALLER;
QUIT;

PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_TODASLASPIEZAS AS 
   SELECT t3.IDEXPEDIENTE, 
          /* PIEZASAUTORIZADAS */
            (COUNT(t3.IDEXPEDIENTE)) AS PIEZASAUTORIZADAS, 
          /* PIEZASENTREGADAS */
            (sum(ifn(t3.CDRCOTIZADOR=1,IFN(t3.FECENTREGAREFACCIONARIA NOT IS MISSING,1,0),ifn(t3.FECHAACTUALIZACION not 
            is missing and t3.FECHAACTUALIZACION <> t3.FECHAEXPEDICION,1,0)))) LABEL="PIEZASENTREGADAS" AS 
            PIEZASENTREGADAS
      FROM WORK.PZA t3
      WHERE t3.IDVALUACION = '01' AND t3.IDVALEESTATUS = '01'
      GROUP BY t3.IDEXPEDIENTE;
QUIT;

PROC SQL;
   CREATE TABLE WORK.'MIN[EXPEDICION]_0000'n AS 
   SELECT t2.IDEXPEDIENTE, 
          t2.CDRCOTIZADOR, 
          t2.CDRAUTOSURTIDO, 
          t2.LLAVEPIEZA, 
          /* Min(FecExpedicion) */
            (MIN(t2.FECHAEXPEDICION)) FORMAT=DATETIME27.6 AS 'Min(FecExpedicion)'n
      FROM WORK.PZA t2
      GROUP BY t2.IDEXPEDIENTE,
               t2.CDRCOTIZADOR,
               t2.CDRAUTOSURTIDO,
               t2.LLAVEPIEZA;
QUIT;

PROC SQL;
   CREATE TABLE WORK.'MAX[ENTREGA]_0000'n AS 
   SELECT t1.LLAVEPIEZA, 
          /* Max(FecEntrega) */
            (MAX(t1.FECENTREGAREFACCIONARIA)) FORMAT=DATETIME27.6 AS 'Max(FecEntrega)'n
      FROM WORK.PZA t1
      GROUP BY t1.LLAVEPIEZA;
QUIT;

/*  */
PROC SQL;
   CREATE TABLE WORK.CONSOLIDADOFEC AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.CDRCOTIZADOR, 
          t1.CDRAUTOSURTIDO, 
          t1.LLAVEPIEZA, 
          t1.'Min(FecExpedicion)'n, 
          t2.'Max(FecEntrega)'n, 
          /* tEntregaPieza(Hab) */
            (INTCK('DTWEEKDAY',t1.'Min(FecExpedicion)'n,t2.'Max(FecEntrega)'n)) AS 
            'tEntregaPieza(Hab)'n, 
          /* tEntregaPieza(Nat) */
            ((t2.'Max(FecEntrega)'n-t1.'Min(FecExpedicion)'n)/86400) FORMAT=5.1 AS 
            'tEntregaPieza(Nat)'n, 
          /* PiezaEntregada */
            (ifn(t2.'Max(FecEntrega)'n is missing,0,1)) AS PiezaEntregada
      FROM WORK.'MIN[EXPEDICION]_0000'n t1
           INNER JOIN WORK.'MAX[ENTREGA]_0000'n t2 ON (t1.LLAVEPIEZA = t2.LLAVEPIEZA)
      GROUP BY t1.IDEXPEDIENTE,
               t1.CDRCOTIZADOR,
               t1.CDRAUTOSURTIDO,
               t1.LLAVEPIEZA,
               t1.'Min(FecExpedicion)'n,
               t2.'Max(FecEntrega)'n;
QUIT;

/*  */
PROC SQL;
   CREATE TABLE WORK.ConsolidadoFechasPiezas AS 
   SELECT DISTINCT t1.IDEXPEDIENTE, 
          t1.CDRCOTIZADOR, 
          t1.CDRAUTOSURTIDO, 
          /* SUM{tEntregaPieza(Hab)} */
            (SUM(t1.'tEntregaPieza(Hab)'n)) AS 'SUM{tEntregaPieza(Hab)}'n, 
          /* SUM{tEntregaPieza(Nat)} */
            (SUM(t1.'tEntregaPieza(Nat)'n)) FORMAT=5.1 AS 'SUM{tEntregaPieza(Nat)}'n, 
          /* SUM(PiezaEntregada) */
            (SUM(t1.PiezaEntregada)) AS 'SUM(PiezaEntregada)'n
      FROM WORK.CONSOLIDADOFEC t1
      GROUP BY t1.IDEXPEDIENTE,
               t1.CDRCOTIZADOR,
               t1.CDRAUTOSURTIDO;
QUIT;  

/*  */
PROC SQL;
   CREATE TABLE WORK.PrimerEnvioAS 
   SELECT t3.IDEXPEDIENTE, 
          t2.FECADJUDICACION, 
          t3.MIN_of_FECHAENVIOTALLER AS PrimerEnvioCDR, 
          /* HORAS_PRIMERENVIO */
            (intck('dtsecond',t2.FECADJUDICACION,t3.min_of_fechaenviotaller)/3600) AS HORAS_PRIMERENVIO, 
          /* ExpedienteVálidoTiempos */
            (IFN(t3.'ExpedienteVálidoTiempos'n = 0 OR t2.fecadjudicacion is missing or intck('dtsecond'
            ,t2.fecadjudicacion,t3.min_of_fechaenviotaller) < -600,0,1)) AS 'ExpedienteVálidoTiempos'n
      FROM VALPROD.FECHAS t2
           INNER JOIN WORK.MINIMOENVIOCDR t3 ON (t2.IDEXPEDIENTE = t3.IDEXPEDIENTE);
QUIT;

/* */
PROC SQL;
   CREATE TABLE WORK.TiempoTotalCDR AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.FECADJUDICACION, 
          t1.PrimerEnvioCDR, 
          t1.HORAS_PRIMERENVIO, 
          t2.horasTallerComplementos, 
          /* tiempoTotalCDR */
            (sum(t1.HORAS_PRIMERENVIO,t2.horasTallerComplementos)) FORMAT=6.1 AS tiempoTotalCDR, 
          /* ExpedienteDesviacion */
            (ifn(t1.'ExpedienteVálidoTiempos'n=0 or t2.expValido=0,1,0))  AS ExpedienteDesviacion
      FROM WORK.PRIMERENVIO t1
           LEFT JOIN WORK.TIEMPOTALLERCOMPLEMENTOS t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE);
QUIT;

/*  */
PROC SQL;
   CREATE TABLE WORK.TotalTiemposValuacion AS 
   SELECT t1.IDEXPEDIENTE, 
          t1.FECADJUDICACION, 
          t1.PrimerEnvioCDR, 
          t3.FEC_PRIMERA_ASIG_CARR, 
          t1.HORAS_PRIMERENVIO, 
          t1.horasTallerComplementos, 
          t1.tiempoTotalCDR, 
          t2.TiempoPrimeraAutorizacion, 
          t2.'TiempoAutorizaciónComplementos'n, 
          t2.'tiempoTotalAutorización'n, 
          t3.TiempoPrimerCarrusel, 
          t3.tiempoCarrComp, 
          t3.tiempoTotalCarrusel, 
          /* tiempoPrimeraAutValuador */
            (t2.TiempoPrimeraAutorizacion-t3.TiempoPrimerCarrusel) FORMAT=6.1 AS tiempoPrimeraAutValuador, 
          /* tiempoCompAutValuador */
            (t2.'TiempoAutorizaciónComplementos'n-t3.tiempoCarrComp) FORMAT=6.1 AS tiempoCompAutValuador, 
          /* tiempoTotalAutValuador */
            (t2.'tiempoTotalAutorización'n-t3.tiempoTotalCarrusel) FORMAT=6.1 AS tiempoTotalAutValuador, 
          t4.ENVIOS, 
          t1.ExpedienteDesviacion
      FROM WORK.TIEMPOTOTALCDR t1
           LEFT JOIN WORK.TIEMPOTOTALVALUACION t2 ON (t1.IDEXPEDIENTE = t2.IDEXPEDIENTE)
           LEFT JOIN WORK.TIEMPOTOTALCARRUSEL t3 ON (t1.IDEXPEDIENTE = t3.IDEXPEDIENTE)
           LEFT JOIN WORK.CUENTAENVIOS t4 ON (t1.IDEXPEDIENTE = t4.IDEXPEDIENTE);
QUIT;

/*  */
PROC SQL;
   CREATE TABLE WORK.QUERY_FOR_DATOSGENERALES_0000 AS 
   SELECT t1.idExp, 
          t1.Ejercicio, 
          t1.Reporte, 
          t1.Siniestro LENGTH=8 AS Siniestro, 
          t1.TIPORIESGO, 
          t1.CODAFECTADO, 
          t1.CODIGOASEGURADO, 
          t1.NUMPOLIZA, 
          t1.NUMENDOSO, 
          t1.NUMINCISO, 
          t1.NOMCONDUCTOR, 
          t3.CAUSA, 
          t3.CAUSA_HOMOLOGADA, 
          t1.CAUSACODIGO AS CAUSACODIGO_DG, 
          t1.CAUSADESCRIP AS CAUSADESCRIP_DG, 
          t1.MarcaVehiculo, 
          t1.TIPO, 
          t1.MODELO, 
          t1.Color, 
          t1.PLACAS, 
          t1.SERIE, 
          t1.UNIDAD, 
          t1.CLIENTE AS CLIENTE, 
          t1.OFICINA, 
          t1.'GRUPO/NEGOCIO'n, 
          t3.CVE_AGENTE, 
          t3.Agente AS Agente, 
          t1.AGENTE AS AGENTE_DG, 
          t3.Gerente, 
          t3.Oficina  AS 'OficinaEmisión'n, 
          t1.OFIEMICODIGO AS Oficina_Emision_DG, 
          t1.OFIEMIDESCRIP AS Ofi_Emi_DG, 
          t3.'Oficina Beneficiario'n AS OficinaSiniestros, 
          t3.Director, 
          t1.PRESUPUESTOMOB, 
          t1.PIEZASCAMBIO, 
          t1.SUMAASEG_DG, 
          t1.MONTODEDUCIBLE_DG, 
          t1.TRANSITO, 
          t1.PISO, 
          t1.Transito_Activo, 
          t1.Piso_Activo, 
          t1.GRUA, 
          t1.Complemento, 
          t3.REF, 
          t3.MO, 
          t3.'Importe Reparaciones'n, 
          t1.CLAVETALLER, 
          t6.Asignados, 
          t6.Transitos, 
          t6.Pisos, 
          t6.Pendiente_Transito, 
          t1.CODVALUADOR, 
        
          t1.HerramientaValuacion LENGTH=8 AS HerramientaValuacion, 
          t1.EstatusValuacion, 
          t1.TipoValuador LENGTH=8 AS TipoValuador, 
          t1.Bandeja, 
          t3.'Fecha Captura'n AS FECHACAPTURA, 
          t7.FECSINIESTRO, 
          t7.FECHAOCURRIDOSISE, 
          t1.FECASIGNACION, 
          t1.FECADJUDICACION, 
          t1.FECENVIO, 
          t1.FecValuacion, 
          t2.MAX_of_FECHAACTUALIZACION_FPR AS FecModificacion, 
          t1.FECINGRESO, 
          t1.PRIMERTERMINO, 
          t1.PRIMERENTREGA, 
          t1.FECTERMINADO, 
          t1.FECENTREGADO, 
          t1.FECHAESTENT, 
          t1.UltimoEnvioCDR, 
          t7.FECAUTORIZACION, 
          t1.PrimeraAutorizacion, 
          t1.UltimaAutorizacion, 
          t5.Reingreso_Portal, 
          t1.DIFDIASTERMINO, 
          t1.DiasDifEnvioAutorizacion, 
          t1.FecAutMayor, 
          t1.DiasReparacion, 
          t1.EstatusExpediente, 
          t1.DiasEnv, 
          t1.RegionValuacion, 
          t1.EstadoCDR, 
          t1.Pob_Comer, 
          t1.CLAVESUPERVISOR, 
          t1.NOMBRENNALISTA, 
          t1.TipoCDR_Portal AS TipoCDR_Portal, 
          t1.TipoCDR, 
          t1.MarcaCDR AS MarcaCDR, 
          t1.NombreCDR AS NombreCDR, 
          t1.Nom_CDR_Comer, 
          t1.CAMBIOTERMINO, 
          t1.VehEntregado, 
          t1.TablaSupervisor, 
          t1.SinSiniestro, 
          /* FecPromesa */
            (ifn(
                t2.IDEXPEDIENTE = t1.idExp,
                t2.MAX_of_FECHAPROMESAREAL,
                t1.FECPROMESA
            )) FORMAT=DATETIME16. AS FecPromesa, 
          /* CambioFecha */
            (ifn(t1.idExp=t2.IDEXPEDIENTE,1,0)) AS CambioFecha, 
          /* DiasValuacion */
            (case when t1.fecenvio not is missing and t1.fecvaluacion not is missing
            then intck('DTWEEKDAY',t1.FECENVIO,t1.FecValuacion)
            end) AS DiasValuacion, 
          t1.SUBRAMO
      FROM WORK.QUERY_FOR_DATOSGENERALES t1
           LEFT JOIN WORK.QUERY_FOR_FECHAPROMESAREALANLCDR t2 ON (t1.idExp = t2.IDEXPEDIENTE)
           LEFT JOIN WORK.QUERY_FOR_VALUACION t3 ON (t1.VALUACION = t3.VALUACION)
           LEFT JOIN WORK.REINGRESO t5 ON (t1.idExp = t5.IDEXPEDIENTE)
           LEFT JOIN WORK.BANDEJA t6 ON (t1.CLAVETALLER = t6.CLAVETALLER)
           LEFT JOIN VALPROD.FECHAS t7 ON (t1.idExp = t7.IDEXPEDIENTE);
QUIT;

/* */
PROC SQL;
   CREATE TABLE WORK.DG_SUPER_SERVICIO AS 
   SELECT DISTINCT t1.idExp, 
          t1.Ejercicio, 
          t1.Reporte, 
          t1.Siniestro, 
          t1.TIPORIESGO, 
          t1.CODAFECTADO, 
          t1.CODIGOASEGURADO, 
          t1.NUMPOLIZA, 
          t1.NUMENDOSO, 
          t1.NUMINCISO, 
          t1.NOMCONDUCTOR, 
          t1.CAUSA, 
          t1.CAUSA_HOMOLOGADA, 
          t1.CAUSACODIGO_DG, 
          t1.CAUSADESCRIP_DG, 
          t1.MarcaVehiculo, 
          t1.TIPO, 
          t1.MODELO, 
          t1.Color, 
          t1.PLACAS, 
          t1.SERIE, 
          t1.UNIDAD, 
          t1.CLIENTE, 
          t1.OFICINA, 
          t1.'GRUPO/NEGOCIO'n, 
          t1.CVE_AGENTE, 
          t1.Agente, 
          t1.AGENTE_DG, 
          t1.Gerente, 
          t1.'OficinaEmisión'n, 
          t1.Oficina_Emision_DG, 
          t1.Ofi_Emi_DG, 
          t1.OficinaSiniestros, 
          t1.Director, 
          t1.PRESUPUESTOMOB, 
          t1.PIEZASCAMBIO, 
          t1.SUMAASEG_DG, 
          t1.MONTODEDUCIBLE_DG, 
          t1.TRANSITO, 
          t1.PISO, 
          t1.Transito_Activo, 
          t1.Piso_Activo, 
          t1.GRUA, 
          t1.Complemento, 
          t1.REF, 
          t1.MO, 
          t1.'Importe Reparaciones'n, 
          t1.CLAVETALLER, 
          t1.Asignados, 
          t1.Transitos, 
          t1.Pisos, 
          t1.Pendiente_Transito, 
          t1.CODVALUADOR, 
     
          t1.HerramientaValuacion, 
          t1.EstatusValuacion, 
          t1.TipoValuador, 
          t1.Bandeja, 
          t1.FECHACAPTURA, 
          t1.FECSINIESTRO, 
          t1.FECHAOCURRIDOSISE, 
          t1.FECASIGNACION, 
          t1.FECADJUDICACION, 
          t1.FECENVIO, 
          t1.FECVALUACION, 
          t1.FecModificacion, 
          t1.FECINGRESO, 
          t1.PRIMERTERMINO, 
          t1.PRIMERENTREGA, 
          t1.FECTERMINADO, 
          t1.FECENTREGADO, 
          t1.FECHAESTENT, 
          t1.UltimoEnvioCDR, 
          t1.FECAUTORIZACION, 
          t1.PrimeraAutorizacion, 
          t1.UltimaAutorizacion, 
          t1.Reingreso_Portal, 
          t1.DIFDIASTERMINO, 
          t1.DiasDifEnvioAutorizacion, 
          t1.FecAutMayor, 
          t1.DiasReparacion, 
          t1.EstatusExpediente, 
          t1.DiasEnv, 
          t1.RegionValuacion, 
          t1.EstadoCDR, 
          t1.Pob_Comer, 
          t1.CLAVESUPERVISOR, 
          t1.NOMBRENNALISTA, 
          t1.TipoCDR_Portal, 
          t1.TipoCDR, 
          t1.MarcaCDR, 
          t1.NombreCDR, 
          t1.Nom_CDR_Comer, 
          t1.CAMBIOTERMINO, 
          t1.VehEntregado, 
          t1.TablaSupervisor, 
          t1.SinSiniestro, 
          t1.FecPromesa, 
          t1.CambioFecha, 
          t1.DiasValuacion, 
          t2.CLAVEANALISTA_SUPSERV, 
          t2.NOMBRE_SUPERSERV, 
          t1.SUBRAMO, 
          t1.FecPromesa AS FecPromesa1, 
          /* REAL_CE */
            (CASE
            WHEN t3.IDEXPEDIENTE IS NOT NULL THEN 1
            ELSE 0
            END) AS REAL_CE
      FROM WORK.QUERY_FOR_DATOSGENERALES_0000 t1
           LEFT JOIN WORK.SUPERVISOR_SERVICIO t2 ON (t1.CLAVETALLER = t2.CLAVETALLER)
           LEFT JOIN WORK.REAL_CE t3 ON (t1.idExp = t3.IDEXPEDIENTE);
QUIT;

/*  */
PROC SQL;
   CREATE TABLE WORK.SUPERVISOR_ AS 
   SELECT DISTINCT t1.idExp, 
          t1.Ejercicio, 
          t1.Reporte, 
          t1.Siniestro, 
          t1.CODAFECTADO, 
          t1.TIPORIESGO, 
          t1.CODIGOASEGURADO, 
          t1.NUMPOLIZA, 
          t1.NUMENDOSO, 
          t1.NUMINCISO, 
          t1.NOMCONDUCTOR, 
          t1.CAUSA, 
          t1.CAUSA_HOMOLOGADA, 
          t1.CAUSACODIGO_DG, 
          t1.CAUSADESCRIP_DG, 
          t1.MarcaVehiculo, 
          t1.TIPO, 
          t1.MODELO, 
          t1.Color, 
          t1.PLACAS, 
          t1.SERIE, 
          t1.UNIDAD, 
          t1.CLIENTE, 
          t1.OFICINA, 
          t1.'GRUPO/NEGOCIO'n, 
          t1.CVE_AGENTE, 
          t1.Agente, 
          t1.AGENTE_DG, 
          t1.Gerente, 
          t1.'OficinaEmisión'n, 
          t1.Oficina_Emision_DG, 
          t1.Ofi_Emi_DG, 
          t1.OficinaSiniestros, 
          t1.Director, 
          t1.PRESUPUESTOMOB, 
          t1.PIEZASCAMBIO, 
          t1.SUMAASEG_DG, 
          t1.MONTODEDUCIBLE_DG, 
          t1.TRANSITO, 
          t1.PISO, 
          t1.Transito_Activo, 
          t1.Piso_Activo, 
          t1.GRUA, 
          t1.Complemento, 
          t1.CLAVETALLER, 
          t1.Asignados, 
          t1.Transitos, 
          t1.Pisos, 
          t1.Pendiente_Transito, 
          t1.CODVALUADOR, 
          
          t1.HerramientaValuacion, 
          t1.EstatusValuacion, 
          t1.TipoValuador, 
          t1.Bandeja, 
          t3.ExpedienteDesviacion, 
          t5.MO, 
          t5.REF, 
          t1.FECSINIESTRO, 
          t1.FECHAOCURRIDOSISE, 
          t1.FECASIGNACION, 
          t1.FECADJUDICACION, 
          t1.FECADJUDICACION LABEL="Fec_Adjudicacion" AS Fec_Adjudicacion, 
          t3.PrimerEnvioCDR, 
          t1.FECENVIO, 
          t1.FecValuacion, 
          t1.FecModificacion, 
          t1.FECINGRESO, 
          t1.FECINGRESO LABEL="Fec_Ingreso" AS Fec_Ingreso, 
          t1.FECTERMINADO, 
          t1.PRIMERTERMINO, 
          t1.PRIMERENTREGA, 
          t1.PRIMERTERMINO AS PRIMERTERMINO1, 
          t1.FECTERMINADO LABEL="Fec_Terminado" AS Fec_Terminado, 
          t1.FECENTREGADO, 
          t1.FECPROMESA1, 
          t1.UltimoEnvioCDR, 
          t1.FECAUTORIZACION, 
          t1.PrimeraAutorizacion, 
          t1.UltimaAutorizacion, 
          /* Reingreso_Portal */
            (case when t1.Reingreso_Portal is missing then 0 else t1.Reingreso_Portal end) AS Reingreso_Portal, 
          t2.PIEZASAUTORIZADAS, 
          t2.PIEZASENTREGADAS, 
          t1.DiasDifEnvioAutorizacion, 
          t1.DIFDIASTERMINO, 
          t1.FecAutMayor, 
          t1.DiasReparacion, 
          t1.DiasValuacion, 
          t1.EstatusExpediente, 
          t1.DiasEnv, 
          t1.RegionValuacion, 
          t1.EstadoCDR, 
        
          t1.Pob_Comer, 
          t1.CLAVESUPERVISOR, 
          t1.NOMBRENNALISTA, 
          t1.CLAVEANALISTA_SUPSERV, 
          t1.NOMBRE_SUPERSERV, 
          t1.TipoCDR_Portal, 
          t1.TipoCDR, 
          t1.MarcaCDR, 
          t1.NombreCDR, 
          t1.Nom_CDR_Comer, 
          t1.TablaSupervisor, 
          t4.CDRCOTIZADOR, 
          t4.CDRAUTOSURTIDO, 
          /* FecPromesa */
            (IFN (t1.FecPromesa1> t1.FECHAESTENT, t1.FecPromesa1, ifn (t1.FECHAESTENT is missing, t1.FecPromesa1, 
            t1.FECHAESTENT))) FORMAT=DATETIME16. AS FecPromesa, 
          t1.CambioFecha, 
          t1.CAMBIOTERMINO, 
          t3.HORAS_PRIMERENVIO LABEL="tiempoPrimerEnvio" AS tiempoPrimerEnvio, 
          t3.horasTallerComplementos LABEL="tiempoTallerComplementos" AS tiempoTallerComplementos, 
          t3.tiempoTotalCDR, 
          t3.TiempoPrimeraAutorizacion, 
          t3.'tiempoTotalAutorización'n, 
          t3.'TiempoAutorizaciónComplementos'n, 
          t3.TiempoPrimerCarrusel, 
          t3.tiempoCarrComp, 
          t3.tiempoTotalCarrusel, 
          t3.tiempoPrimeraAutValuador, 
          t3.tiempoCompAutValuador, 
          t3.tiempoTotalAutValuador, 
          t3.ENVIOS, 
          t4.'SUM{tEntregaPieza(Hab)}'n, 
          t4.'SUM{tEntregaPieza(Nat)}'n, 
          t4.'SUM(PiezaEntregada)'n, 
          /* ExpValidoTiempos */
            (ifn(t3.IDEXPEDIENTE <> t1.idexp,0,1)) LABEL="ExpValidoTiempos" AS ExpValidoTiempos, 
          /* AAMM_Adj */
            (case when month(datepart(t1.fecadjudicacion)) < 10
            then cat(year(datepart(t1.fecadjudicacion)),'_0',month(datepart(t1.fecadjudicacion)))
            else cat(year(datepart(t1.fecadjudicacion)),'_',month(datepart(t1.fecadjudicacion)))
            end) LABEL="AAMM_Adj" AS AAMM_Adj, 
          /* AAMM_Env */
            (case when month(datepart(t1.fecenvio)) < 10
            then cat(year(datepart(t1.fecenvio)),'_0',month(datepart(t1.fecenvio)))
            else cat(year(datepart(t1.fecenvio)),'_',month(datepart(t1.fecenvio)))
            end) LABEL="AAMM_Env" AS AAMM_Env, 
          /* ExpEnviado */
            (case when t1.FECENVIO not is missing then 1 else 0 end) LABEL="ExpEnviado" AS ExpEnviado, 
          /* RankEnvio */
            (case when t1.diasenv not is missing and t1.diasenv>=0 then
                case when t1.DiasEnv = 0 then 'A'
                        when t1.diasenv = 1 then 'B' else 'C' end
            else
                case when intck('DTWEEKDAY',datetime(),t1.fecadjudicacion) = 0 then 'A'
                        when intck('DTWEEKDAY',datetime(),t1.fecadjudicacion) = 1 then 'B'
                        when intck('DTWEEKDAY',datetime(),t1.fecadjudicacion) > 1 then 'C'
                end
            end) LABEL="RankEnvio" AS RankEnvio, 
          /* VehTerminado */
            (case when t1.FECTERMINADO not is missing then 1 else 0 end) LABEL="VehTerminado" AS VehTerminado, 
          /* VehTerminadoPrimeraVez */
            (case when t1.PRIMERTERMINO not is missing then 1 else 0 end) AS VehTerminadoPrimeraVez, 
          /* RankPromesa */
            (case when t1.fecingreso not is missing then
            case when t1.fecpromesa not is missing then
                case when t1.fecterminado not is missing then
                    case when t1.FECTERMINADO<= (t1.FecPromesa + 86400)  then 'Cumplió promesa' else 
            'No cumplió promesa' end
                else
                    case when datetime() - (t1.FecPromesa + 86400) <= 0 then 'Fecha promesa en tiempo' else 
            'Fecha promesa vencida' end
                end
            else 'SIN FECHA PROMESA'
            end
            else 'Sin fecha ingreso'
            end) LABEL="RankPromesa" AS RankPromesa, 
          /* DiasPromesaVencida */
            (case when t1.fecingreso not is missing then
            case when t1.fecterminado is missing and t1.fecpromesa not is missing and datetime()>t1.fecpromesa then 
            (datetime()-t1.FecPromesa)/86400 end
            end) FORMAT=BEST5.2 LABEL="DiasPromesaVencida" AS DiasPromesaVencida, 
          /* nExp */
            (1) LABEL="nExp" AS nExp, 
          /* tCorrido */
            (case when t1.fecterminado not is missing then
                 case when t1.fecpromesa - t1.fecterminado < 0 then intck('dtweekday1w',t1.fecpromesa,t1.fecterminado) 
            else intck('dtweekday1w',t1.fecterminado,t1.fecpromesa) end
            else
                 case when t1.fecpromesa - datetime() < 0 then intck('dtweekday1w',t1.fecpromesa,datetime()) else intck(
            'dtweekday1w',datetime(),t1.fecpromesa) end
            end) FORMAT=12.1 LABEL="tCorrido" AS tCorrido, 
          /* DiasRepReal_PrimerTermino */
            (case
            when t1.EstatusValuacion='REPARACION' and t1.primertermino not is missing and t1.primertermino >= 
            t1.fecingreso then intck('dtweekday1w',t1.fecingreso,t1.primertermino) 
            when t1.EstatusValuacion='REPARACION' and t1.FECTERMINADO is not missing and  t1.primertermino< 
            t1.fecingreso and  t1.FECTERMINADO >= t1.fecingreso then intck('dtweekday1w',t1.fecingreso,t1.FECTERMINADO)
            end) LABEL="DiasRepReal_PrimerTermino" AS DiasRepReal_PrimerTermino, 
          /* DiasRepReal */
            (case when t1.FECTERMINADO not is missing and t1.fecterminado > t1.fecingreso then intck('dtweekday1w'
            ,t1.fecingreso,t1.fecterminado) end) FORMAT=5.1 LABEL="DiasRepReal" AS DiasRepReal, 
          /* DifDiasRep */
            (case when t1.fecterminado not is missing then intck('dtweekday1w',t1.fecingreso,t1.fecterminado) - 
            t1.DiasReparacion end) FORMAT=5.1 LABEL="DifDiasRep" AS DifDiasRep, 
          /* CumplePromesa */
            (case when t1.fecingreso not is missing then
                 case when t1.fecpromesa not is missing then
                      case when t1.fecterminado not is missing then
                           case when t1.FECTERMINADO<=(t1.FecPromesa+86400) then 1
            end
            end
            end
            end) LABEL="CumplePromesa" AS CumplePromesa, 
          /* NoCumplePromesa */
            (case when t1.fecingreso not is missing then
                 case when t1.fecpromesa not is missing then
                     case when t1.fecterminado not is missing then
                         case when t1.FECTERMINADO > (t1.FecPromesa + 86400) then 1
            end
            end
            end
            end) LABEL="NoCumplePromesa" AS NoCumplePromesa, 
          /* CumplePromesa_PrimerTermino */
            (case when t1.fecingreso not is missing then
                 case when t1.fecpromesa not is missing then
                      case when t1.primertermino not is missing then
                           case when t1.PRIMERTERMINO<=(t1.FecPromesa+86400) then 1
            end
            end
            end
            end) AS CumplePromesa_PrimerTermino, 
          /* NoCumplePromesa_PrimerTermino */
            (case when t1.fecingreso not is missing then
                 case when t1.fecpromesa not is missing then
                     case when t1.primertermino not is missing then
                         case when t1.PRIMERTERMINO > (t1.FecPromesa + 86400) then 1
            end
            end
            end
            end) AS NoCumplePromesa_PrimerTermino, 
          /* PromesaVencida */
            (case when t1.fecingreso not is missing then
                 case when t1.fecterminado is missing then
                      case when t1.fecpromesa not is missing then
                            case when (t1.fecpromesa + 86400) <= datetime() then 1 end
                      end
                 end
            end) LABEL="PromesaVencida" AS PromesaVencida, 
          /* PromesaPorVencer */
            (case when t1.fecingreso not is missing then
                 case when t1.fecpromesa not is missing then
                      case when t1.fecterminado is missing then
                           case when (t1.fecpromesa + 86400) > datetime() then 1 end
                      end
                 end
            end) LABEL="PromesaPorVencer" AS PromesaPorVencer, 
          /* Vencido */
            (case when t1.fecingreso not is missing then
                 case when t1.fecpromesa not is missing then
                      case when t1.fecterminado not is missing then
                           case when t1.fecterminado > (t1.fecpromesa + 86400) then 1 end
                      else
                           case when datetime() > (t1.fecpromesa + 86400) then 1 end
                      end
                 end
            end
            ) LABEL="Vencido" AS Vencido, 
          /* EnTiempo */
            (case when t1.fecingreso not is missing then
            case when t1.fecpromesa not is missing then
            case when t1.fecterminado not is missing then
            case when t1.fecterminado <= t1.fecpromesa then 1 end
            else
            case when datetime() <= t1.fecpromesa then 1 end
            end
            end
            end) LABEL="EnTiempo" AS EnTiempo, 
          /* VehiculoIngresado */
            (ifn(t1.fecingreso not is missing,1,0)) LABEL="VehiculoIngresado" AS VehiculoIngresado, 
          /* VehNoTerminado */
            (case when t1.FECTERMINADO is missing then 1 else 0 end) LABEL="VehNoTerminado" AS VehNoTerminado, 
          t1.VehEntregado, 
          /* DiasSinTerminar */
            (case when t1.FECTERMINADO is missing then intck('dtweekday1w',t1.fecingreso,datetime()) end) LABEL=
            "DiasSinTerminar" AS DiasSinTerminar, 
          /* DiasProceso */
            (intck('dtweekday1w',t1.FECADJUDICACION,t1.FECTERMINADO)) AS DiasProceso, 
          /* +30 DIAS */
            (case when t1.estatusvaluacion = 'REPARACION' and t1.fecingreso not is missing then ifn((datetime() - 
            t1.fecingreso)/86400 > 30,1,0) end) LABEL="+30 DIAS" AS '+30 DIAS'n, 
          /* DiasDesdeIngreso */
            ((datetime()-fecingreso)/86400) FORMAT=BEST5.1 LABEL="DiasDesdeIngreso" AS DiasDesdeIngreso, 
          /* DifDiasRep_PrimerTermino */
            (case when t1.primertermino not is missing then intck('dtweekday1w',t1.fecingreso,t1.primertermino) - 
            t1.DiasReparacion end) LABEL="DifDiasRep_PrimerTermino" AS DifDiasRep_PrimerTermino, 
          /* CATEGORIAVALUACION */
            (case when t1.TipoValuador='AUTOS' then
            (case
            when t2.piezasautorizadas is missing then 'V0 - 0 piezas'
            when t2.piezasautorizadas > 0 and t2.piezasautorizadas <= 3 then 'V1 - 1 a 3 piezas'
            when t2.piezasautorizadas > 3 and t2.piezasautorizadas <= 10 then 'V2 - 4 a 10 piezas'
            when t2.piezasautorizadas > 10 and t2.piezasautorizadas <= 23 then 'V3 - 11 a 23 piezas'
            when t2.piezasautorizadas > 23 then 'V4 - más de 24 piezas'
            else ''
            end)
            when  t1.TipoValuador='EP' then (
            case when t5.MO+t5.REF=0 THEN 'VEP0' 
            when t5.MO+t5.REF>0 AND t5.MO+t5.REF<=35000 then 'VEP1'
            WHEN t5.MO+t5.REF>35000 AND t5.MO+t5.REF<=110000 THEN 'VEP2'
            WHEN t5.MO+t5.REF>110000 AND t5.MO+t5.REF<=200000 THEN 'VEP3' 
            WHEN t5.MO+t5.REF>200000 THEN 'VEP4'
            ELSE ''
            END)
            END) LABEL="CATEGORIAVALUACION" AS CATEGORIAVALUACION, 
          /* tiempoPrimeraValuación */
            (case when t1.FECENVIO < t1.fecvaluacion then intck('dtsecond',t1.FECENVIO,t1.FECVALUACION)/3600 end) LABEL=
            "tiempoPrimeraValuación" AS 'tiempoPrimeraValuación'n, 
          /* ExpedienteInvestigación */
            (IFN(t6.IDEXPEDIENTE = t1.idExp,1,0)) LABEL="ExpedienteInvestigación" AS 'ExpedienteInvestigación'n, 
          t1.SinSiniestro, 
         
          /* TIPO_VE */
            (case
            when t2.piezasautorizadas is missing then 'V0'
            when t2.piezasautorizadas > 0 and t2.piezasautorizadas <= 3 then CATS('V1,',t2.PIEZASAUTORIZADAS)
            when t2.piezasautorizadas > 3 and t2.piezasautorizadas <= 10 then CATS('V2,',t2.PIEZASAUTORIZADAS)
            when t2.piezasautorizadas > 10 and t2.piezasautorizadas <= 23 then CATS('V3,',t2.PIEZASAUTORIZADAS)
            when t2.piezasautorizadas > 23 then CATS('V4,',t2.PIEZASAUTORIZADAS)
            else ''
            end) AS TIPO_VE, 
          /* DIAS META */
            (case
            when (case when t1.TipoValuador='AUTOS' then
            (case
            when t2.piezasautorizadas is missing then 'V0 - 0 piezas'
            when t2.piezasautorizadas > 0 and t2.piezasautorizadas <= 3 then 'V1 - 1 a 3 piezas'
            when t2.piezasautorizadas > 3 and t2.piezasautorizadas <= 10 then 'V2 - 4 a 10 piezas'
            when t2.piezasautorizadas > 10 and t2.piezasautorizadas <= 23 then 'V3 - 11 a 23 piezas'
            when t2.piezasautorizadas > 23 then 'V4 - más de 24 piezas'
            else ''
            end)
            when  t1.TipoValuador='EP' then (
            case when t5.MO+t5.REF=0 THEN 'VEP0' 
            when t5.MO+t5.REF>0 AND t5.MO+t5.REF<=35000 then 'VEP1'
            WHEN t5.MO+t5.REF>35000 AND t5.MO+t5.REF<=110000 THEN 'VEP2'
            WHEN t5.MO+t5.REF>110000 AND t5.MO+t5.REF<=200000 THEN 'VEP3' 
            WHEN t5.MO+t5.REF>200000 THEN 'VEP4'
            ELSE ''
            END)
            END) = 'V0 - 0 piezas' then 2.95
            When (case when t1.TipoValuador='AUTOS' then
            (case
            when t2.piezasautorizadas is missing then 'V0 - 0 piezas'
            when t2.piezasautorizadas > 0 and t2.piezasautorizadas <= 3 then 'V1 - 1 a 3 piezas'
            when t2.piezasautorizadas > 3 and t2.piezasautorizadas <= 10 then 'V2 - 4 a 10 piezas'
            when t2.piezasautorizadas > 10 and t2.piezasautorizadas <= 23 then 'V3 - 11 a 23 piezas'
            when t2.piezasautorizadas > 23 then 'V4 - más de 24 piezas'
            else ''
            end)
            when  t1.TipoValuador='EP' then (
            case when t5.MO+t5.REF=0 THEN 'VEP0' 
            when t5.MO+t5.REF>0 AND t5.MO+t5.REF<=35000 then 'VEP1'
            WHEN t5.MO+t5.REF>35000 AND t5.MO+t5.REF<=110000 THEN 'VEP2'
            WHEN t5.MO+t5.REF>110000 AND t5.MO+t5.REF<=200000 THEN 'VEP3' 
            WHEN t5.MO+t5.REF>200000 THEN 'VEP4'
            ELSE ''
            END)
            END) = 'V1 - 1 a 3 piezas' then 4.4
            when (case when t1.TipoValuador='AUTOS' then
            (case
            when t2.piezasautorizadas is missing then 'V0 - 0 piezas'
            when t2.piezasautorizadas > 0 and t2.piezasautorizadas <= 3 then 'V1 - 1 a 3 piezas'
            when t2.piezasautorizadas > 3 and t2.piezasautorizadas <= 10 then 'V2 - 4 a 10 piezas'
            when t2.piezasautorizadas > 10 and t2.piezasautorizadas <= 23 then 'V3 - 11 a 23 piezas'
            when t2.piezasautorizadas > 23 then 'V4 - más de 24 piezas'
            else ''
            end)
            when  t1.TipoValuador='EP' then (
            case when t5.MO+t5.REF=0 THEN 'VEP0' 
            when t5.MO+t5.REF>0 AND t5.MO+t5.REF<=35000 then 'VEP1'
            WHEN t5.MO+t5.REF>35000 AND t5.MO+t5.REF<=110000 THEN 'VEP2'
            WHEN t5.MO+t5.REF>110000 AND t5.MO+t5.REF<=200000 THEN 'VEP3' 
            WHEN t5.MO+t5.REF>200000 THEN 'VEP4'
            ELSE ''
            END)
            END) =  'V2 - 4 a 10 piezas' then 12.4
            when (case when t1.TipoValuador='AUTOS' then
            (case
            when t2.piezasautorizadas is missing then 'V0 - 0 piezas'
            when t2.piezasautorizadas > 0 and t2.piezasautorizadas <= 3 then 'V1 - 1 a 3 piezas'
            when t2.piezasautorizadas > 3 and t2.piezasautorizadas <= 10 then 'V2 - 4 a 10 piezas'
            when t2.piezasautorizadas > 10 and t2.piezasautorizadas <= 23 then 'V3 - 11 a 23 piezas'
            when t2.piezasautorizadas > 23 then 'V4 - más de 24 piezas'
            else ''
            end)
            when  t1.TipoValuador='EP' then (
            case when t5.MO+t5.REF=0 THEN 'VEP0' 
            when t5.MO+t5.REF>0 AND t5.MO+t5.REF<=35000 then 'VEP1'
            WHEN t5.MO+t5.REF>35000 AND t5.MO+t5.REF<=110000 THEN 'VEP2'
            WHEN t5.MO+t5.REF>110000 AND t5.MO+t5.REF<=200000 THEN 'VEP3' 
            WHEN t5.MO+t5.REF>200000 THEN 'VEP4'
            ELSE ''
            END)
            END) =  'V3 - 11 a 23 piezas' then 24
            When (case when t1.TipoValuador='AUTOS' then
            (case
            when t2.piezasautorizadas is missing then 'V0 - 0 piezas'
            when t2.piezasautorizadas > 0 and t2.piezasautorizadas <= 3 then 'V1 - 1 a 3 piezas'
            when t2.piezasautorizadas > 3 and t2.piezasautorizadas <= 10 then 'V2 - 4 a 10 piezas'
            when t2.piezasautorizadas > 10 and t2.piezasautorizadas <= 23 then 'V3 - 11 a 23 piezas'
            when t2.piezasautorizadas > 23 then 'V4 - más de 24 piezas'
            else ''
            end)
            when  t1.TipoValuador='EP' then (
            case when t5.MO+t5.REF=0 THEN 'VEP0' 
            when t5.MO+t5.REF>0 AND t5.MO+t5.REF<=35000 then 'VEP1'
            WHEN t5.MO+t5.REF>35000 AND t5.MO+t5.REF<=110000 THEN 'VEP2'
            WHEN t5.MO+t5.REF>110000 AND t5.MO+t5.REF<=200000 THEN 'VEP3' 
            WHEN t5.MO+t5.REF>200000 THEN 'VEP4'
            ELSE ''
            END)
            END) = 'V4 - más de 24 piezas' then 41
            end
            ) AS 'DIAS META'n, 
          t1.SUBRAMO, 
          /* Calculation */
            (case
            when t1.SUBRAMO='01' then "AUTOS"
            when t1.SUBRAMO='02' then "EP"
            when t1.SUBRAMO='03' then "TURISTAS"
            end) AS Calculation, 
          /* GerenciaValuacion */
            (CASE 	
                WHEN t1.TipoValuador = 'AUTOS' THEN 	
                    CASE   WHEN t1.EstadoCDR IN ('CHIHUAHUA', 'BAJA CALIFORNIA', 'BAJA CALIFORNIA SUR',  'SONORA', 
            'DURANGO') THEN 'NOROESTE'	
                        WHEN t1.EstadoCDR IN ('DURANGO', 'JALISCO', 'NAYARIT', 'COLIMA', 'SINALOA',  'ZACATECAS', 
            'AGUASCALIENTES',  'GUANAJUATO', 'MICHOACAN') THEN 'OCCIDENTE'	
                        WHEN t1.EstadoCDR IN ('CAMPECHE', 'OAXACA', 'TABASCO', 'YUCATAN', 'CHIAPAS', 'VERACRUZ', 
            'PUEBLA', 'QUINTANA ROO', 'TLAXCALA' ) THEN 'CENTRO SUR'	
                        WHEN t1.EstadoCDR IN ('CDMX I', 'REGION 15') THEN 'CENTRO' 	
                        WHEN t1.EstadoCDR IN ('TAMAULIPAS',  'SAN LUIS POTOSI', 'NUEVO LEON', 'COAHUILA') THEN 
            'NORESTE'	
                        WHEN t1.EstadoCDR IN ('EDO. DE MEXICO', 'QUERETARO', 'HIDALGO',  'GUERRERO', 'MORELOS') THEN 
            'PERIFERIA' 	
                        ELSE " "	
                    END	
                WHEN t1.TipoValuador = 'EP' THEN	
                    CASE 	
                        WHEN t1.EstadoCDR IN ('CDMX I', 'EDO. DE MEXICO', 'GUERRERO', 'HIDALGO', 'MORELOS', 'PUEBLA', 
            'TLAXCALA') THEN 'CENTRO EP'	
                        WHEN t1.EstadoCDR IN ('AGUASCALIENTES', 'QUERETARO', 'SAN LUIS POTOSI', 'GUANAJUATO') THEN 
            'BAJIO EP'	
                        WHEN t1.EstadoCDR IN ('CHIHUAHUA', 'COAHUILA', 'DURANGO', 'NUEVO LEON', 'TAMAULIPAS', 
            'ZACATECAS') THEN 'NORTE EP'	
                        WHEN t1.EstadoCDR IN ('COLIMA', 'JALISCO', 'MICHOACAN', 'NAYARIT') THEN 'OCCIDENTE EP'	
                        WHEN t1.EstadoCDR IN ('BAJA CALIFORNIA', 'BAJA CALIFORNIA SUR', 'CAMPECHE', 'CHIAPAS', 'OAXACA'
            , 'QUINTANA ROO', 'SINALOA', 'SONORA', 'TABASCO', 'VERACRUZ', 'YUCATAN') THEN 'PENINSULAR EP'	
                       END	
                       END	
            ) AS GerenciaValuacion, 
          /* PRO_ORDER */
            (CASE
            WHEN t1.CLAVETALLER IN 
            (
            '09732',	
            '11331',	
            '06686',	
            '06839',	
            '07164',	
            '07190',	
            '07301',	
            '07480',	
            '07849',	
            '08166',	
            '50774',	
            '50914',	
            '50944',	
            '50985',	
            '51491',	
            '52301',	
            '52410',	
            '52451',	
            '52507',	
            '52531',	
            '52550',	
            '52746',	
            '97085',	
            '97335',	
            '97531',	
            '97544',	
            '04968',	
            '05286',	
            '05703',	
            '12906',	
            '13198',	
            '14102',	
            '14115',	
            '14689',	
            '14963',	
            '15303',	
            '15351',	
            '15375',	
            '15396',	
            '16024',	
            '57183',	
            '57621',	
            '61619',	
            '61623',	
            '61707',	
            '97671',	
            '98050',	
            '98207',	
            '98752',	
            '00887',	
            '00900',	
            '01012',	
            '01621',	
            '01643',	
            '02500',	
            '03301',	
            '03493',	
            '04276',	
            '04431',	
            '04779',	
            '50349',	
            '50648',	
            '50690',	
            '50733',	
            '01953',	
            '16534',	
            '16549',	
            '16571',	
            '16654',	
            '16253',	
            '16366',	
            '16418',	
            '16509',	
            '12297',	
            '01320',	
            '12814',	
            '50009',	
            '50160',	
            '50264',	
            '52821',	
            '16972',	
            '16749',	
            '17146',	
            '16275',	
            '57557',	
            '17097',	
            '50347',	
            '12773',	
            '17398',	
            '14457',	
            '96627',	
            '17289',	
            '17881',	
            '98751',	
            '17668',	
            '17231',	
            '18527',	
            '18064',	
            '18680',	
            '18139',	
            '02529',	
            '17947',	
            '18504',	
            '18598',	
            '18162',	
            '18626',	
            '18654',	
            '18903',	
            '19483',	
            '19061',	
            '19394',	
            '18994',	
            '19689',	
            '19533',	
            '19851',	
            '19700',	
            '19962',	
            '19760',	
            '19617',	
            '20116',	
            '20513',	
            '20668',	
            '21084',	
            '20915',	
            '21369',	
            '52646',	
            '21387',	
            '21398',	
            '20998',	
            '21498',	
            '05529',	
            '19694',	
            '22006',	
            '21543',	
            '21649',	
            '21981',	
            '21598',	
            '21553',	
            '21661',	
            '22362',	
            '22424',	
            '22378',	
            '22702',	
            '23144',	
            '22805',	
            '23141',	
            '22523',	
            '23051',	
            '23037',	
            '25338',	
            '23323',	
            '23656',	
            '24150',	
            '22703',	
            '27116',	
            '25971',	
            '22798',	
            '23241',	
            '23826',	
            '25642',	
            '24827',	
            '25186',	
            '23305',	
            '23805',	
            '25593',	
            '26417',	
            '24633',	
            '26269',	
            '23398',	
            '25771',	
            '26646',	
            '23269',	
            '03168',	
            '05578',	
            '22255',	
            '23944',	
            '26411',	
            '24566',	
            '25207',	
            '25343',	
            '61705',	
            '23464',	
            '24983',	
            '23894',	
            '26706',	
            '13363',	
            '25738',	
            '25730',	
            '26673',	
            '11178',	
            '27049',	
            '27027',	
            '23610',	
            '19900',	
            '21647',	
            '23824',	
            '22421',	
            '22227',	
            '23982',	
            '23712',	
            '22851',	
            '26965',	
            '25379',	
            '27042',	
            '24058',	
            '18106',	
            '26973',	
            '22654',	
            '25630',	
            '26155',	
            '23239',	
            '25701',	
            '23330',	
            '25605',	
            '27043',	
            '26191',	
            '26612',	
            '23588',	
            '24129',	
            '24140',	
            '23240',	
            '27960',	
            '15990',	
            '29272',	
            '29618',	
            '28805',	
            '28833',	
            '06966',	
            '28905',	
            '29408',	
            '27458',	
            '27259',	
            '27410',	
            '28011',	
            '27642',	
            '27475',	
            '20185',	
            '27637',	
            '28435',	
            '28943',	
            '29047',	
            '12721',	
            '29183',	
            '30401',	
            '28537',	
            '28538',	
            '30559',	
            '27504',	
            '27528',	
            '30549',	
            '14528',	
            '29629',	
            '02917',	
            '27683',	
            '29270',	
            '28840',	
            '27545',	
            '28896',	
            '27258',	
            '23322',	
            '30329',	
            '27942',	
            '30486',	
            '29421',	
            '29958',	
            '29818',	
            '27759',	
            '04027',	
            '16127',	
            '30613',	
            '28182',	
            '29176',	
            '29544',	
            '27900',	
            '29300',	
            '28206',	
            '29057',	
            '29627',	
            '28660',	
            '28533',	
            '28795',	
            '98363',	
            '30333',	
            '22401',	
            '29647',	
            '27890',	
            '27907',	
            '52224',	
            '28855',	
            '29380',	
            '03847',	
            '30414',	
            '28085',	
            '29429',	
            '30343',	
            '28065',	
            '30272',	
            '28836',	
            '28824',	
            '30550',	
            '27963',	
            '27531',	
            '29046',	
            '28176',	
            '28154',	
            '30416',	
            '29019',	
            '31724',	
            '33494',	
            '31693',	
            '33759',	
            '33895',	
            '33972',	
            '31197',	
            '33407',	
            '33506',	
            '31350',	
            '33845',	
            '33892',	
            '32628',	
            '33191',	
            '09368',	
            '31162',	
            '33757',	
            '32787',	
            '32407',	
            '33250',	
            '34331',	
            '31190',	
            '33984',	
            '34455',	
            '30652',	
            '32014',	
            '31960',	
            '31390',	
            '33594',	
            '32425',	
            '33573',	
            '32493',	
            '34354',	
            '33906',	
            '34023',	
            '33869',	
            '32228',	
            '33080',	
            '34669',	
            '33581',	
            '31128',	
            '31725',	
            '33591',	
            '34918',	
            '35351',	
            '35359',	
            '35360',	
            '31471',	
            '31419',	
            '32788',	
            '33803',	
            '31726',	
            '33484',	
            '32782',	
            '32834',	
            '33985',	
            '34458',	
            '33483',	
            '33020',	
            '31710',	
            '30728',	
            '33282',	
            '33789',	
            '34917',	
            '33007',	
            '32699',	
            '33735',	
            '33879',	
            '34457',	
            '34509',	
            '34549',	
            '33533',	
            '34985',	
            '32558',	
            '34670',	
            '32612',	
            '31201',	
            '32686',	
            '32726',	
            '34353',	
            '32815',	
            '33052',	
            '35034',	
            '32530',	
            '33386',	
            '34819',	
            '34454',	
            '35289',	
            '37141',	
            '37601',	
            '37965',	
            '38177',	
            '40349',	
            '40902',	
            '40968',	
            '37487',	
            '38052',	
            '38342',	
            '38343',	
            '39401',	
            '35780',	
            '38439',	
            '38732',	
            '39332',	
            '39722',	
            '40201',	
            '38011',	
            '38312',	
            '38919',	
            '40319',	
            '40327',	
            '40452',	
            '40631',	
            '40632',	
            '37646',	
            '39983',	
            '40306',	
            '40649',	
            '40890',	
            '36356',	
            '40661',	
            '36045',	
            '38303',	
            '38383',	
            '36242',	
            '37964',	
            '36364',	
            '38420',	
            '38565',	
            '39954',	
            '41142',	
            '36590',	
            '37867',	
            '38717',	
            '38742',	
            '39408',	
            '39620',	
            '40468',	
            '36939',	
            '37534',	
            '38423',	
            '39518',	
            '35676',	
            '37641',	
            '38656',	
            '37872',	
            '38429',	
            '39405',	
            '39083',	
            '39570',	
            '39843',	
            '40365',	
            '40841',	
            '41352',	
            '36372',	
            '37212',	
            '37909',	
            '37633',	
            '38731',	
            '39771',	
            '41192',	
            '39336',	
            '39442',	
            '41025',	
            '36736',	
            '39569',	
            '38424',	
            '38524',	
            '41281',	
            '37398',	
            '37991',	
            '39339',	
            '39796',	
            '37185',	
            '37301',	
            '39763',	
            '41143',	
            '38316',	
            '40426',	
            '37201',	
            '37966',	
            '38375',	
            '38422',	
            '39216',	
            '40922',	
            '36355',	
            '38853',	
            '39052',	
            '40749',	
            '40813',	
            '37314',	
            '37464',	
            '38556',	
            '39166',	
            '39431',	
            '37550',	
            '38022',	
            '38100',	
            '40298',	
            '40256',	
            '40969',	
            '40317',	
            '36484',	
            '39725',	
            '40180',	
            '40715',	
            '36154',	
            '36516',	
            '38065',	
            '40318',	
            '40460',	
            '40650',	
            '41232',	
            '38531',	
            '38685',	
            '36944',	
            '37972',	
            '39072',	
            '39073',	
            '35869',	
            '36763',	
            '37468',	
            '37607',	
            '40190',	
            '37570',	
            '38863',	
            '40828',	
            '37240',	
            '37645',	
            '37677',	
            '37904',	
            '38530',	
            '38636',	
            '35632',	
            '39258',	
            '40178',	
            '40633',	
            '39368',	
            '40200',	
            '41169',	
            '35873',	
            '35888',	
            '35913',	
            '40281',	
            '41448',	
            '37315',	
            '41361',	
            '36552',	
            '38752',	
            '41324',	
            '38206',	
            '38285',	
            '38756',	
            '40480',	
            '37712',	
            '36061',	
            '38769',	
            '39721',	
            '38923',	
            '37690',	
            '38758',	
            '38630',	
            '39369',	
            '36425',	
            '40202',	
            '37088',	
            '40286',	
            '36533',	
            '36551',	
            '37819',	
            '38629',	
            '41272',	
            '36067',	
            '37295',	
            '38668',	
            '38684',	
            '40659',	
            '36481',	
            '36486',	
            '37840',	
            '40149',	
            '38505',	
            '38783',	
            '39891',	
            '41165',	
            '41437',	
            '37081',	
            '38931',	
            '40481',	
            '41278',	
            '35696',	
            '35700',	
            '36838',	
            '38638',	
            '38751',	
            '40483',	
            '38776',	
            '38784',	
            '39135',	
            '41227',	
            '38663',	
            '40975',	
            '35590',	
            '36553',	
            '36663',	
            '41360',	
            '37739',	
            '36543',	
            '37078',	
            '38231',	
            '39454',	
            '41549',	
            '36136',	
            '36401',	
            '40324',	
            '40621',	
            '40635',	
            '37754',	
            '39333',	
            '40475',	
            '36373',	
            '41289',	
            '38754',	
            '38888',	
            '38198',	
            '41191',	
            '36157',	
            '35929',	
            '35945',	
            '38208',	
            '37775',	
            '38501',	
            '38729',	
            '38759',	
            '39528',	
            '36647',	
            '36752',	
            '40193',	
            '40450',	
            '41436',	
            '36091',	
            '36092',	
            '36052',	
            '36398',	
            '40378',	
            '40751',	
            '36407',	
            '37097',	
            '36664',	
            '41094',	
            '38757',	
            '39889',	
            '39712',	
            '41090',	
            '40320',	
            '41239',	
            '38637',	
            '38640',	
            '38252',	
            '38213',	
            '38229',	
            '38276',	
            '38652',	
            '41089',	
            '36631',	
            '37293',	
            '39453',	
            '40155',	
            '41222',	
            '38650',	
            '39524',	
            '39469',	
            '37359',	
            '37815'	
            
            ) THEN "PRO_ORDER" ELSE "OTROS"
            END	
             ) AS PRO_ORDER, 
          t1.REAL_CE
      FROM WORK.DG_SUPER_SERVICIO t1
           LEFT JOIN WORK.QUERY_FOR_TODASLASPIEZAS t2 ON (t1.idExp = t2.IDEXPEDIENTE)
           LEFT JOIN WORK.TOTALTIEMPOSVALUACION t3 ON (t1.idExp = t3.IDEXPEDIENTE)
           LEFT JOIN WORK.CONSOLIDADOFECHASPIEZAS t4 ON (t1.idExp = t4.IDEXPEDIENTE)
           LEFT JOIN WORK.CONSOLIDAMONTOS t5 ON (t1.idExp = t5.IDEXPEDIENTE)
           LEFT JOIN WORK.EXTRAEEXPEDIENTESINVEST t6 ON (t1.idExp = t6.IDEXPEDIENTE)
      
      WHERE t1.EstatusValuacion IN ('','DA?O MENOR AL DEDUCIBLE','PAGO DE DA?OS','PERDIDA TOTAL','POSIBLE RESCATE',
           'REPARACION','NO PROCEDE')
           AND ( t1.FECASIGNACION <= datetime() AND t1.FECADJUDICACION <= datetime() AND t1.FECENVIO <= datetime() 
                      AND t1.FecValuacion <= datetime() AND t1.FECINGRESO <= datetime() AND t1.FECTERMINADO <= 
           datetime() AND 
                      t1.FECENTREGADO <= datetime() ) AND t3.ExpedienteDesviacion NOT IS MISSING AND t1.FECSINIESTRO >= 
           '1Jan2022:0:0:0'dt;
QUIT;

/* Guardado en INDBASE */
PROC SQL;
   CREATE TABLE INDBASE.SUPERVISOR_CDR AS 
   SELECT t1.idExp, 
          t1.Ejercicio, 
          t1.Reporte, 
          t1.Siniestro, 
          t1.CODAFECTADO, 
          t1.TIPORIESGO, 
          t1.CODIGOASEGURADO, 
          t1.NUMPOLIZA, 
          t1.NUMENDOSO, 
          t1.NUMINCISO, 
          t1.NOMCONDUCTOR, 
          t1.CAUSA, 
          t1.CAUSA_HOMOLOGADA, 
          t1.CAUSACODIGO_DG, 
          t1.CAUSADESCRIP_DG, 
          t1.MarcaVehiculo, 
          t1.TIPO, 
          t1.MODELO, 
          t1.Color, 
          t1.PLACAS, 
          t1.SERIE, 
          t1.UNIDAD, 
          t1.CLIENTE, 
          t1.OFICINA, 
          t1.'GRUPO/NEGOCIO'n, 
          t1.CVE_AGENTE, 
          t1.Agente, 
          t1.AGENTE_DG, 
          t1.Gerente, 
          t1.'OficinaEmisión'n, 
          t1.Oficina_Emision_DG, 
          t1.Ofi_Emi_DG, 
          t1.OficinaSiniestros, 
          t1.Director, 
          t1.PRESUPUESTOMOB, 
          t1.PIEZASCAMBIO, 
          t1.SUMAASEG_DG, 
          t1.MONTODEDUCIBLE_DG, 
          t1.TRANSITO, 
          t1.PISO, 
          t1.Transito_Activo, 
          t1.Piso_Activo, 
          t1.GRUA, 
          t1.Complemento, 
          t1.CLAVETALLER, 
          t1.Asignados, 
          t1.Transitos, 
          t1.Pisos, 
          t1.Pendiente_Transito, 
          t1.CODVALUADOR, 
      
          t1.HerramientaValuacion, 
          t1.EstatusValuacion, 
          t1.TipoValuador, 
          t1.Bandeja, 
          t1.ExpedienteDesviacion, 
          t1.MO, 
          t1.REF, 
          t1.FECSINIESTRO, 
          t1.FECHAOCURRIDOSISE, 
          t1.FECASIGNACION, 
          t1.FECADJUDICACION, 
          t1.Fec_Adjudicacion, 
          t1.PrimerEnvioCDR, 
          t1.FECENVIO, 
          t1.FECVALUACION, 
          t1.FecModificacion, 
          t1.FECINGRESO, 
          t1.Fec_Ingreso, 
          t1.FECTERMINADO, 
          t1.PRIMERTERMINO, 
          t1.PRIMERENTREGA, 
          t1.PRIMERTERMINO1, 
          t1.Fec_Terminado, 
          t1.FECENTREGADO, 
          t1.FecPromesa1, 
          t1.UltimoEnvioCDR, 
          t1.FECAUTORIZACION, 
          t1.PrimeraAutorizacion, 
          t1.UltimaAutorizacion, 
          t1.Reingreso_Portal, 
          t1.PIEZASAUTORIZADAS, 
          t1.PIEZASENTREGADAS, 
          t1.DiasDifEnvioAutorizacion, 
          t1.DIFDIASTERMINO, 
          t1.FecAutMayor, 
          t1.DiasReparacion, 
          t1.DiasValuacion, 
          t1.EstatusExpediente, 
          t1.DiasEnv, 
          t1.RegionValuacion, 
          t1.EstadoCDR, 
          
          t1.Pob_Comer, 
          t1.CLAVESUPERVISOR, 
          t1.NOMBRENNALISTA, 
          t1.CLAVEANALISTA_SUPSERV, 
          t1.NOMBRE_SUPERSERV, 
          t1.TipoCDR_Portal, 
          t1.TipoCDR, 
          t1.MarcaCDR, 
          t1.NombreCDR, 
          t1.Nom_CDR_Comer, 
          t1.TablaSupervisor, 
          t1.CDRCOTIZADOR LABEL='', 
          t1.CDRAUTOSURTIDO LABEL='', 
          t1.FecPromesa, 
          t1.CambioFecha, 
          t1.CAMBIOTERMINO, 
          t1.tiempoPrimerEnvio, 
          t1.tiempoTallerComplementos, 
          t1.tiempoTotalCDR, 
          t1.TiempoPrimeraAutorizacion, 
          t1.'tiempoTotalAutorización'n, 
          t1.'TiempoAutorizaciónComplementos'n, 
          t1.TiempoPrimerCarrusel, 
          t1.tiempoCarrComp, 
          t1.tiempoTotalCarrusel, 
          t1.tiempoPrimeraAutValuador, 
          t1.tiempoCompAutValuador, 
          t1.tiempoTotalAutValuador, 
          t1.ENVIOS, 
          t1.'SUM{tEntregaPieza(Hab)}'n, 
          t1.'SUM{tEntregaPieza(Nat)}'n, 
          t1.'SUM(PiezaEntregada)'n, 
          t1.ExpValidoTiempos, 
          t1.AAMM_Adj, 
          t1.AAMM_Env, 
          t1.ExpEnviado, 
          t1.RankEnvio, 
          t1.VehTerminado, 
          t1.VehTerminadoPrimeraVez, 
          t1.RankPromesa, 
          t1.DiasPromesaVencida, 
          t1.nExp, 
          t1.tCorrido, 
          t1.DiasRepReal_PrimerTermino, 
          t1.DiasRepReal, 
          t1.DifDiasRep, 
          t1.CumplePromesa, 
          t1.NoCumplePromesa, 
          t1.CumplePromesa_PrimerTermino, 
          t1.NoCumplePromesa_PrimerTermino, 
          t1.PromesaVencida, 
          t1.PromesaPorVencer, 
          t1.Vencido, 
          t1.EnTiempo, 
          t1.VehiculoIngresado, 
          t1.VehNoTerminado, 
          t1.VehEntregado, 
          t1.DiasSinTerminar, 
          t1.DiasProceso, 
          t1.'+30 DIAS'n, 
          t1.DiasDesdeIngreso, 
          t1.DifDiasRep_PrimerTermino, 
          t1.CATEGORIAVALUACION, 
          t1.'tiempoPrimeraValuación'n, 
          t1.'ExpedienteInvestigación'n, 
          t1.SinSiniestro, 
          t1.'Valuacion Estadistica'n, 
          t1.TIPO_VE, 
          t1.'DIAS META'n, 
          t1.SUBRAMO, 
          t1.Calculation, 
          t1.GerenciaValuacion, 
          t1.PRO_ORDER, 
          /* GERENCIA_NAC */
            (CASE
            WHEN t1.GerenciaValuacion in ('NORTE' 'PENINSULAR OCCIDENTE' 'OCCIDENTE') then 
            "GERENCIA NACIONAL I - GABRIEL HERNANDEZ"
            WHEN t1.GerenciaValuacion in ('CENTRO' 'CENTRO SUR' 'SURESTE') then "GERENCIA NACIONAL II - ISAAC GASCA" 
            ELSE " "
            END) AS GERENCIA_NAC, 
          t2.CUENTA, 
          t2.EJECUTIVA, 
          t1.REAL_CE
      FROM WORK.SUPERVISOR_ t1
           LEFT JOIN CONVENB.EJECUTIVAS_SEG t2 ON (t1.CVE_AGENTE = t2.'CVE AGENTE'n);
QUIT;