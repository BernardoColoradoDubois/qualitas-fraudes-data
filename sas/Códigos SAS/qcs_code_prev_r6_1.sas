**********************************************************************************************************
* AUTOR: SAS Institute Mexico                                                                  *
* FECHA: Septiembre 3, 2021                                                                              *
* TITULO: Creación de Prep-Table Familia 6 para la regla 6.1                                             *
* OBJETIVO: Determinar siniestros cuyo porcentaje de recuperación sea menor al 80%                       *
* ENTRADAS: APERCAB_BSC, RECUPERACIONES, VALUACION, ORDEN_BSC                                            *
* SALIDAS: QCS_PREP_PREV_R6_1_ENGINE, QCS_PREP_PREV_R6_1_DETAIL, QCS_PREP_PREV_R6_1_RESULT               *
**********************************************************************************************************;
*------------------------*
* Parametros de sistema  *
*------------------------*;

*--------------------------*
* Asignación de librerias  *
*--------------------------*;
%include '/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_librerias_rutas2.sas';

*----------------------------------------------*
* Obtiene la lista de exclusion de Ajustadores *
*----------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AJUSTADOR AS
    SELECT PARAMETER_VALUE
 FROM &lib_resu..QCS_PARAM_PREV_R6_1
	WHERE NAME_OF_THE_RULE='PREVENCION 6.1' AND PARAMETER_NAME='LISTA_AJUSTADOR';
QUIT;
RUN;

/*Busca el valor en los nombres de ajustador estandarizado*/
proc sql;
 create table LISTA_AJUSTADOR2 as 
select  distinct
    PARAMETER_VALUE
   ,  kstrip(u.CLAVE_AJUSTADOR) ||'_' || kstrip(u.NOMBRE_AJUSTADOR_GR) as CVE_NOMBRE_AJUSTADOR_GR length=360
  from LISTA_AJUSTADOR l
inner join 
      (
      select CLAVE_AJUSTADOR
            ,NOMBRE_AJUSTADOR_GR
            , kstrip(CLAVE_AJUSTADOR) || '_' || kstrip(NOMBRE_AJUSTADOR) as cve_nom_ajustador_origen    
            from   &lib_resu..QCS_AGENTE_UNICO ) u
on  compress(upcase(kstrip(l.PARAMETER_VALUE)), , "kw")  =compress(upcase(kstrip(u.cve_nom_ajustador_origen)), , "kw");
quit;

*-------------------------------------------------------------*
* Obtiene la lista de inclusion de Codigos de Responsabilidad *
*-------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_COD_RESP AS
    SELECT PARAMETER_VALUE FROM &lib_resu..QCS_PARAM_PREV_R6_1
	WHERE NAME_OF_THE_RULE='PREVENCION 6.1' AND PARAMETER_NAME='LISTA_COD_RESP';
QUIT;
RUN;
*------------------------------------------------------*
* Obtiene el umbral para el Porcentaje de Recuperacion *
*------------------------------------------------------*;
PROC SQL NOPRINT;
    SELECT PARAMETER_VALUE INTO :UMBRAL_PCT_RECUP FROM &lib_resu..QCS_PARAM_PREV_R6_1
	WHERE NAME_OF_THE_RULE='PREVENCION 6.1' AND PARAMETER_NAME='UMBRAL_PCT_RECUP';
QUIT;


/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses
  estas variable son de control solo para el periodo de carga
*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

*------------------------------------------------------------------------------------*
* Obtiene las fechas para identificar el periodo de analisis de los ultimos 12 meses *
* y la fecha mas reciente de ocurrido el siniestro para tomarse como fecha de corte  *
*------------------------------------------------------------------------------------*;
PROC SQL noprint;
connect to oracle as Insumos
(path=&path_db. user=&user_db. password=&pwd_user_db.);
select FEC_REP, FEC_OCU into:MAX_FEC_REP, :FEC_CIERRE
from connection to Insumos
(select max(FEC_REP) as FEC_REP, max(FEC_OCU) as FEC_OCU from APERCAB_BSC);
disconnect from Insumos;
QUIT;

%LET COMMA=%STR(%');
DATA _NULL_;
CALL SYMPUT("DOCE_MESES_ANTES",CATS("&COMMA.",PUT(%SYSFUNC(INTNX(DTMONTH,"&MAX_FEC_REP."dt,-11,B)),DATETIME20.),"&COMMA."));
RUN;
%PUT &=DOCE_MESES_ANTES;
*-----------------------------------------------------------------*
* Identifica los siniestros donde el asegurado no es responsable  *
*-----------------------------------------------------------------*;
PROC SQL;
connect to oracle as Insumos
( path=&path_db. user=&user_db. password=&pwd_user_db.);
create table WORK.SINIESTROS_T1 as
select REPORTE,
       FEC_REP,
	   FEC_OCU,
       CVE_AJUS,
	   SINIESTRO,
	   TIP_RESP_ORI,
	   COD_RESP,
       INTCK('DTDAY', FEC_OCU, "&FEC_CIERRE."dt) as DIAS_ANTIGUEDAD 
from connection to Insumos
(select REPORTE,  
        FEC_REP,
		FEC_OCU,
        CVE_AJUS,
		SINIESTRO,
		TIP_RESP_ORI,
		COD_RESP
   from APERCAB_BSC
 where TIP_RESP_ORI = '02' and FEC_REP >= to_date(&DOCE_MESES_ANTES.,'DDMONYYYY:HH24:MI:SS'))
/*Incluye los codigos de responsabilidad de la lista de parametros*/
where COD_RESP in (select distinct PARAMETER_VALUE from Work.LISTA_COD_RESP) 
;
disconnect from Insumos;
QUIT;


*----------------------------------------------------------------------------------------------------*
* Identificar los siniestros donde existe recuperación por ajustador con valuación de Daño Material  *
*----------------------------------------------------------------------------------------------------*;
PROC SQL;
connect to oracle as Insumos
(
path=&path_db. user=&user_db. password=&pwd_user_db.
);
create table WORK.SINIESTROS_R_0 as
select CLAVE_AJUSTADOR,
	   NOMBRE_AJUSTADOR,
       COMPRESS(RAMO||ANIO||SINIESTRO) AS SINIESTRO,
	   TIPO,
	   COBERTURA,
	   TIPO_INGRESO,
	   CODIGO_INGRESO, /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
	   MONTO_INGRESO,
       RAMO,
       ANIO,
       MON_ING,
       T_CAMBIO
from connection to Insumos
(select CLAVE_AJUSTADOR,
		NOMBRE_AJUSTADOR,
        RAMO,
        ANIO,
        SINIESTRO,
		TIPO,
		COBERTURA,
		TIPO_INGRESO,
        CODIGO_INGRESO, /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
        MONTO_INGRESO,
        RAMO,
        ANIO,
        MON_ING,
        T_CAMBIO
   from RECUPERACIONES
  );
disconnect from Insumos;
QUIT;
  
data SINIESTROS_R_0 (drop=tmp_COBERTURA);
	set SINIESTROS_R_0 (rename=(COBERTURA=tmp_COBERTURA 
		MONTO_INGRESO=tmp_MONTO_INGRESO) );
	length COBERTURA  $60.;

     var_unique=_N_;

	/*Estandariza nombre de cobertura */
	if kstrip(upcase(COBERTURA)) in ('DANOS MAT.', 'DANOS MAT') then
		do;
			COBERTURA='DANOS MAT';
		end;
	else
		do;
			COBERTURA=tmp_COBERTURA;
		end;

	/*convertierte todos las recuperaciones ingresadas a moneda Nacional*/
	if upcase(kstrip(MON_ING)) eq 'DLS' then
		do;
			MONTO_INGRESO=tmp_MONTO_INGRESO * T_CAMBIO;

			if round(T_CAMBIO, .000001) <= 0 or  missing(T_CAMBIO) = 1  then
				do;
					put "Nota: MONTO_INGRESO=" MONTO_INGRESO "Tipo de cambio <= 0   T_CAMBIO=" T_CAMBIO;
				end;
        end;
	    else 
				do;
					MONTO_INGRESO=tmp_MONTO_INGRESO;
				end;

    if missing(kstrip(CLAVE_AJUSTADOR)) = 0;
run;

/*Asocia GR de nombre de ajustador*/  
proc sql;
 create table SINIESTROS_R_1 as 
  select  t3.*
  , case
      when missing(u.NOMBRE_AJUSTADOR_GR) = 0 then  u.NOMBRE_AJUSTADOR_GR
      else NOMBRE_AJUSTADOR
  end  as  NOMBRE_AJUSTADOR_GR length=300
 , kstrip(t3.CLAVE_AJUSTADOR) || '_' || kstrip( calculated NOMBRE_AJUSTADOR_GR)  as CVE_NOMBRE_AJUSTADOR_GR length=360
 from  SINIESTROS_R_0  t3
    left join  ( select distinct CLAVE_AJUSTADOR, NOMBRE_AJUSTADOR_GR from  &lib_resu..QCS_AGENTE_UNICO ) u
    on kstrip(t3.CLAVE_AJUSTADOR)  = kstrip(u.CLAVE_AJUSTADOR);
quit;

proc sort data=SINIESTROS_R_1 nodupkey;
 by var_unique;
quit;

/*Agrupa MONTO_INGRESO por la llave  CVE_NOMBRE_AJUSTADOR_GR, SINIESTRO, TIPO, COBERTURA, TIPO_INGRESO, CODIGO, INGRESO */
proc sql;  
create table SINIESTROS_R_2 as 
  select 
       CVE_NOMBRE_AJUSTADOR_GR,
       NOMBRE_AJUSTADOR_GR,
       CLAVE_AJUSTADOR,
	   NOMBRE_AJUSTADOR,
       SINIESTRO,
	   TIPO,
	   COBERTURA,
	   TIPO_INGRESO,
	   CODIGO_INGRESO, /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
      SUM( MONTO_INGRESO ) AS MONTO_INGRESO
  from  SINIESTROS_R_1
 group by  CVE_NOMBRE_AJUSTADOR_GR, SINIESTRO, TIPO, COBERTURA, TIPO_INGRESO, CODIGO_INGRESO; /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
quit; 
  
/*Toma el primer nombre de ajustador dado la clave de ajustador tuviera más de un nombre de origen*/ 

proc sort data=SINIESTROS_R_2 out=SINIESTROS_T3 nodupkey;
 by CVE_NOMBRE_AJUSTADOR_GR SINIESTRO TIPO COBERTURA TIPO_INGRESO CODIGO_INGRESO; /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
quit;

*------------------------------------------------------------------*
* Obtiene total de valuacion de los daños iniciales del asegurado  *
*------------------------------------------------------------------*;
PROC SQL;
connect to oracle as Insumos
(path=&path_db. user=&user_db. password=&pwd_user_db.);
create table WORK.SINIESTROS_T4 as
select * from connection to Insumos
(select 
        AJUSTADOR,
		SINIESTRO,
		ASEG_TERCERO,
        COBERTURA AS COBERTURA_VALUACION,
        DEDUCIBLE,
        SUM(TOTAL_VALUACION) AS TOTAL_VALUACION
   from VALUACION
 where ASEG_TERCERO = 'ASEGURADO' AND COBERTURA = '01' AND DEDUCIBLE = 0
 group by AJUSTADOR, SINIESTRO, ASEG_TERCERO, COBERTURA, DEDUCIBLE);
disconnect from Insumos;
QUIT;

*----------------------------------------------------------------------------------------------*
* Consolida la informacion de los siniestros con recupero donde el asegurado no es responsable *
*----------------------------------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.SINIESTROS_T5 AS
		SELECT
            t2.CVE_NOMBRE_AJUSTADOR_GR,
            t2.NOMBRE_AJUSTADOR_GR,
            t1.CVE_AJUS AS AJUSTADOR,
			t2.NOMBRE_AJUSTADOR,
            t1.REPORTE,
			t1.SINIESTRO,
			t1.TIP_RESP_ORI,
			t1.COD_RESP,
			t2.TIPO,
			t2.COBERTURA,
            t2.CODIGO_INGRESO, /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
			t2.TIPO_INGRESO,
			t2.MONTO_INGRESO,
            t3.ASEG_TERCERO,
            t3.COBERTURA_VALUACION,
            t3.DEDUCIBLE,
            t3.TOTAL_VALUACION,
            t1.FEC_REP,
			t1.FEC_OCU,
            "&FEC_CIERRE."dt AS FEC_CIERRE FORMAT=DATETIME20.,
			t1.DIAS_ANTIGUEDAD
		FROM
			WORK.SINIESTROS_T1 t1
LEFT JOIN WORK.SINIESTROS_T3 t2 ON (t1.CVE_AJUS=t2.CLAVE_AJUSTADOR AND t1.SINIESTRO = t2.SINIESTRO)
INNER JOIN WORK.SINIESTROS_T4 t3 ON (t1.CVE_AJUS=t3.AJUSTADOR AND t1.SINIESTRO = t3.SINIESTRO)

WHERE
	TIPO = ' ' OR (upcase(TIPO) = 'RECUPERACION' AND COBERTURA in ('DANOS MAT.' 'DANOS MAT') AND CODIGO_INGRESO = '231') AND /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
	/*Descarta los ajustadores contenidos en la lista de exclusion*/
	 upcase(CVE_NOMBRE_AJUSTADOR_GR) not in (select distinct upcase(CVE_NOMBRE_AJUSTADOR_GR)  from Work.LISTA_AJUSTADOR2) 

ORDER BY t1.SINIESTRO;
QUIT;
  
*----------------------------------------------------*
* Descarta los reportes de orden tradicional y SIPAC *
*----------------------------------------------------*;  
PROC SQL;
	CREATE TABLE WORK.SINIESTROS_T6 AS
		SELECT
			*
		FROM
			WORK.SINIESTROS_T5
		WHERE
			MONTO_INGRESO = . AND
            REPORTE NOT IN (SELECT DISTINCT SUBSTR(REPORTE,1,LENGTH(STRIP(REPORTE))-3) FROM &lib_insu..ORDEN_BSC)
	;
QUIT;
RUN;

DATA WORK.SINIESTROS_T7 ;
 SET WORK.SINIESTROS_T5(WHERE=(MONTO_INGRESO NE .))
     WORK.SINIESTROS_T6 ;
RUN;

*------------modificado----------------------------------------*;

/* Librería de Insumos porque la modificacion lo requeria CH 24/06/2025*/
libname insumos oracle path="qcsprd01" user="insumos" password="{SAS002}BE8F4D453B0E1DF041C45724440717E65C923343"
schema="insumos" preserve_tab_names=no preserve_col_names=no
readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;

proc sql;
create table SINIESTROS_T7_1 as
select a.*, b.NOM_ASEGURADO,  b.CONDUCTOR
from WORK.SINIESTROS_T7 a inner join INSUMOS.APERTURA_REPORTE b    /*MODIFICAR LIBRERÍA XM */
on a.REPORTE=b.REPORTE
;
quit;
run;
*----------------------------------------------------*




*----------------------------------------------------*
* Crea las Prep Tables Motora - Detalle - Resultados *****modificado***
*----------------------------------------------------*;
PROC SORT DATA=WORK.SINIESTROS_T7_1 OUT=WORK.QCS_PREP_PREV_R6_1_ENGINE;
  BY SINIESTRO;
RUN;

%put &=UMBRAL_PCT_RECUP;
data QCS_PREP_PREV_R6_1_ENGINE;
	set QCS_PREP_PREV_R6_1_ENGINE;
	flg_porct_recupera=.;

	/*Calcula el porcentaje de recuperacion */
	PCT_RECUPERACION=ifn(missing(MONTO_INGRESO)=1, 0, DIVIDE(MONTO_INGRESO, 
		TOTAL_VALUACION));

	/*Identifica montos menor al porcentaje de recuperación */
	flg_porct_compara = ifn( round(PCT_RECUPERACION, .000001)  &UMBRAL_PCT_RECUP,1,0); 

	/*Identifica siniestros incumplidos por el ajustador y que el nombre del ajustador no sea missing */
	if ((round(PCT_RECUPERACION, .000001)   &UMBRAL_PCT_RECUP ) and 
		missing(CVE_NOMBRE_AJUSTADOR_GR)=0) then
			do;
			flg_incumplido_ajustador=1;
		end;
	else
		do;
			flg_incumplido_ajustador=0;
		end;
run;

PROC SQL;
connect to oracle
( user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");
execute (truncate table QCS_PREP_PREV_R6_1_ENGINE) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &lib_resu..QCS_PREP_PREV_R6_1_ENGINE
   SELECT 
       CVE_NOMBRE_AJUSTADOR_GR,
       NOMBRE_AJUSTADOR_GR, 
       AJUSTADOR,
	   NOMBRE_AJUSTADOR,
	   REPORTE,
	   SINIESTRO,
		NOM_ASEGURADO,
		CONDUCTOR,   
	   TIP_RESP_ORI,
	   COD_RESP,
	   TIPO,
	   COBERTURA,
	   CODIGO_INGRESO, /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
	   TIPO_INGRESO,
	   MONTO_INGRESO,
	   ASEG_TERCERO,
	   COBERTURA_VALUACION,
	   DEDUCIBLE,
	   TOTAL_VALUACION,
	   FEC_REP,
	   FEC_OCU,
	   FEC_CIERRE,
	   DIAS_ANTIGUEDAD,
       FLG_INCUMPLIDO_AJUSTADOR,
	   DATETIME() as BATCHDATE,
	   "&SYSUSERID." AS SYSUSERID,
       DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE

   FROM WORK.QCS_PREP_PREV_R6_1_ENGINE;
QUIT;
RUN;

PROC SQL;
connect to oracle
(user=&user_db2  orapw=&pwd_user_db2 path="&path_db2");
		execute (delete from QCS_PREP_PREV_R6_1_DETAIL 
         	where trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &lib_resu..QCS_PREP_PREV_R6_1_DETAIL /*validar si requieren esta información xm*/
		SELECT
            CVE_NOMBRE_AJUSTADOR_GR,
            NOMBRE_AJUSTADOR_GR, 
			AJUSTADOR,
		    NOMBRE_AJUSTADOR,
		    REPORTE,
		    SINIESTRO,
				NOM_ASEGURADO,
				CONDUCTOR,
		    TIP_RESP_ORI,
		    COD_RESP,
		    TIPO,
		    COBERTURA,
		    CODIGO_INGRESO, /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
		    TIPO_INGRESO,
		    MONTO_INGRESO,
		    ASEG_TERCERO,
		    COBERTURA_VALUACION,
		    DEDUCIBLE,
		    TOTAL_VALUACION,
		    FEC_REP,
		    FEC_OCU,
		    FEC_CIERRE,
	        DIAS_ANTIGUEDAD,
            PCT_RECUPERACION, 
            FLG_INCUMPLIDO_AJUSTADOR,
            DATETIME() as BATCHDATE,
	        "&SYSUSERID." AS SYSUSERID,
	        DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			QCS_PREP_PREV_R6_1_ENGINE t1;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&user_db2  orapw=&pwd_user_db2 path="&path_db2" );
execute (delete from QCS_PREP_PREV_R6_1_DETAIL 
     where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
connect to oracle
(  user=&user_db2  orapw=&pwd_user_db2 path="&path_db2" );
execute (truncate table QCS_PREP_PREV_R6_1_RESULT) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &lib_resu..QCS_PREP_PREV_R6_1_RESULT
		SELECT
            t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.NOMBRE_AJUSTADOR_GR,  
			t1.AJUSTADOR,
			t1.NOMBRE_AJUSTADOR,
            t1.REPORTE,
			t1.SINIESTRO,
				t1.NOM_ASEGURADO,
				t1.CONDUCTOR,
			t1.FEC_OCU,
			t1.DIAS_ANTIGUEDAD,
			t1.MONTO_INGRESO,
            t1.TIP_RESP_ORI,
			t1.COD_RESP,
			t1.ASEG_TERCERO,
			t1.COBERTURA_VALUACION,
            t1.DEDUCIBLE,
			t1.TIPO,
			t1.COBERTURA,
			t1.TOTAL_VALUACION,
			t1.PCT_RECUPERACION,
            t1.FLG_INCUMPLIDO_AJUSTADOR,
            DATETIME() as BATCHDATE,
	        "&SYSUSERID." AS SYSUSERID,
	        DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			 QCS_PREP_PREV_R6_1_ENGINE t1
		WHERE  flg_porct_compara = 1;
QUIT;
RUN;

Libname &lib_resu. clear;
Libname &lib_insu. clear;
cas session_load terminate;

/*PRUEBA DE EJECUCIÓN 18/05/23    */