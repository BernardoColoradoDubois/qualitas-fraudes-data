***********************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                        *
* FECHA: Septiembre 14, 2021                                                                  *
* TITULO: Creación de Prep-Table Familia 6 para la regla 6.4                                  *
* OBJETIVO: Determinar siniestro cuyo recupero es depositado posterior a 5 días               *
* ENTRADAS: APERCAB_BSC, RECUPERACIONES                                                       *
* SALIDAS: QCS_PREP_PREV_R6_4_ENGINE, QCS_PREP_PREV_R6_4_DETAIL, QCS_PREP_PREV_R6_4_RESULT    *
***********************************************************************************************;
*------------------------*
* Parametros de sistema  *
*------------------------*;
Options compress=YES;
*--------------------------*
* Asignación de librerias  *
*--------------------------*;
%Let LIBN_ORA=INSUMOS;
%Let LIBN_SAS=RESULTS;

%Let Ins_ora_user     = insumos;
%Let Ins_ora_password = "{SAS002}BE8F4D453B0E1DF041C45724440717E65C923343";
%Let Ins_ora_schema   = insumos;
%Let Ins_ora_path     = "QCSPRD01";

%Let Res_ora_user     = resultados;
%Let Res_ora_password = "{SAS002}0E91B656514964920CEA6BA629EB49470E5F96D6";
%Let Res_ora_schema   = resultados;
%Let Res_ora_path     = "QCSPRD01";

Libname &LIBN_ORA. oracle path=&Ins_ora_path. schema="&Ins_ora_schema." user="&Ins_ora_user." password=&Ins_ora_password.
preserve_tab_names=no preserve_col_names=no readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;

Libname &LIBN_SAS. oracle path=&Res_ora_path. schema="&Res_ora_schema." user="&Res_ora_user." password=&Res_ora_password.
preserve_tab_names=no preserve_col_names=no readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;

*----------------------------------------------*
* Obtiene la lista de exclusion de Ajustadores *
*----------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AJUSTADOR AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R6_4
	WHERE NAME_OF_THE_RULE='PREVENCION 6.4' AND PARAMETER_NAME='LISTA_AJUSTADOR';
QUIT;
RUN;

/*Busca el valor en los nombres de ajustador estandarizado*/
proc sql;
 create table LISTA_AJUSTADOR2 as 
select  distinct
    PARAMETER_VALUE
   , kstrip(u.CLAVE_AJUSTADOR) || '_' || kstrip( u.NOMBRE_AJUSTADOR_GR ) as CVE_NOMBRE_AJUSTADOR_GR length=360
  from LISTA_AJUSTADOR l
inner join 
      (
      select CLAVE_AJUSTADOR
            ,NOMBRE_AJUSTADOR_GR
            , kstrip(CLAVE_AJUSTADOR)  || '_'  || kstrip(NOMBRE_AJUSTADOR ) as cve_nom_ajustador_origen length=360    
            from   &LIBN_SAS..QCS_AGENTE_UNICO ) u
on  compress(upcase(kstrip(l.PARAMETER_VALUE)), , "kw")  =compress(upcase(kstrip(u.cve_nom_ajustador_origen)), , "kw");
quit;

*------------------------------------------------------------*
* Obtiene el umbral para los Dias de Ocurrencia del Deposito *
*------------------------------------------------------------*;
PROC SQL NOPRINT;
    SELECT PARAMETER_VALUE INTO :UMBRAL_DIAS_OCURRE_DEP FROM &LIBN_SAS..QCS_PARAM_PREV_R6_4
	WHERE NAME_OF_THE_RULE='PREVENCION 6.4' AND PARAMETER_NAME='UMBRAL_DIAS_OCURRE_DEP';
QUIT;
*------------------------------------------------------------------------------------*
* Obtiene las fechas para identificar el periodo de analisis de los ultimos 12 meses *
* y la fecha mas reciente de reporte del siniestro para tomarse como fecha de corte  *
*------------------------------------------------------------------------------------*;
PROC SQL noprint;
connect to oracle as Insumos
(user=&Ins_ora_user. password=&Ins_ora_password. path=&Ins_ora_path.);
select FEC_REP into:MAX_FEC_REP
from connection to Insumos
(select max(FEC_REP) as FEC_REP from APERCAB_BSC);
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
(user=&Ins_ora_user. password=&Ins_ora_password. path=&Ins_ora_path.);
create table WORK.SINIESTROS_T1 as
select * from connection to Insumos
(select FEC_REP,
		CVE_AJUS,
		SINIESTRO,
		TIP_RESP_ORI,
		COD_RESP
   from APERCAB_BSC
 where TIP_RESP_ORI = '02' and COD_RESP = '03' and FEC_REP >= to_date(&DOCE_MESES_ANTES.,'DDMONYYYY:HH24:MI:SS') 
    and trim(CVE_AJUS) IS NOT NULL  ); 
disconnect from Insumos;
QUIT;
*----------------------------------------------------------------------------------------------------*
* Identificar los siniestros donde existe recuperación por ajustador con valuación de Daño Material  *
*----------------------------------------------------------------------------------------------------*;
PROC SQL;
connect to oracle as Insumos
(user=&Ins_ora_user. password=&Ins_ora_password. path=&Ins_ora_path.);
create table WORK.tmp_recuperacion as
select CLAVE_AJUSTADOR,
	   NOMBRE_AJUSTADOR,
       COMPRESS(RAMO||ANIO||SINIESTRO) AS SINIESTRO,
	   TIPO,
	   COBERTURA,
	   TIPO_INGRESO,
       MONTO_INGRESO,
       FECHA_DE_OCURRIDO,
       FEC_DEPOSITO
from connection to Insumos
(select CLAVE_AJUSTADOR,
		NOMBRE_AJUSTADOR,
        RAMO,
        ANIO,
        SINIESTRO,
		TIPO,
		COBERTURA,
		TIPO_INGRESO,
        MONTO_INGRESO,
        FECHA_DE_OCURRIDO,
        FEC_DEPOSITO
   from RECUPERACIONES
  where	TIPO = 'RECUPERACION' AND (upper(trim(COBERTURA)) = 'DANOS MAT.' OR upper(trim(COBERTURA)) = 'DANOS MAT') AND CODIGO_INGRESO = '231'); /*Las columnas CODIGO e INGRESO son una sola FL 28-11-23 */
disconnect from Insumos;
QUIT;

data tmp_recuperacion;
   set tmp_recuperacion;
	length COBERTURA  $60.;
   	/*Estandariza nombre de cobertura */
   	if kstrip(upcase(COBERTURA)) in ('DANOS MAT.', 'DANOS MAT') then
		do;
			COBERTURA='DANOS MAT';
		end;
	else
		do;
			COBERTURA=tmp_COBERTURA;
		end;

   /*Elimina los datos que no tuvieran valor en la clave de ajustador*/
    if missing(CLAVE_AJUSTADOR) = 0 ;
run;

/*Asocia GR de nombre de ajustador*/  
proc sql;
 create table  tmp_recuperacion2 as
   select   
       r.CLAVE_AJUSTADOR,
	   r.NOMBRE_AJUSTADOR,
       r.SINIESTRO,
	   r.TIPO,
	   r.COBERTURA,
	   r.TIPO_INGRESO,
       r.MONTO_INGRESO,
       r.FECHA_DE_OCURRIDO,
       r.FEC_DEPOSITO,
       case
          when missing(u.NOMBRE_AJUSTADOR_GR) = 0 then  u.NOMBRE_AJUSTADOR_GR
          else r.NOMBRE_AJUSTADOR
       end  as  NOMBRE_AJUSTADOR_GR length=160,
          kstrip(r.CLAVE_AJUSTADOR) || '_' ||  kstrip(calculated NOMBRE_AJUSTADOR_GR)  as CVE_NOMBRE_AJUSTADOR_GR length=360
  from  tmp_recuperacion r
   left join  ( select distinct CLAVE_AJUSTADOR, NOMBRE_AJUSTADOR_GR from &LIBN_SAS..QCS_AGENTE_UNICO ) u
    on kstrip(r.CLAVE_AJUSTADOR) =  kstrip(u.CLAVE_AJUSTADOR);  
quit;

/*Descarta los ajustadores contenidos en la lista de exclusion*/
proc sql; 
 create table SINIESTROS_T2 as
  select   
       r.CLAVE_AJUSTADOR,
	   r.NOMBRE_AJUSTADOR,
       r.SINIESTRO,
	   r.TIPO,
	   r.COBERTURA,
	   r.TIPO_INGRESO,
       r.MONTO_INGRESO,
       r.FECHA_DE_OCURRIDO,
       r.FEC_DEPOSITO,
       r.NOMBRE_AJUSTADOR_GR,
       r.CVE_NOMBRE_AJUSTADOR_GR
 from  tmp_recuperacion2 r
  where  upcase(r.CVE_NOMBRE_AJUSTADOR_GR) not in (select distinct upcase(CVE_NOMBRE_AJUSTADOR_GR) from Work.LISTA_AJUSTADOR2);
quit;
 
PROC SQL;
	CREATE TABLE WORK.SINIESTROS_T2_1 AS
		SELECT
            t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.NOMBRE_AJUSTADOR_GR,
			t1.CLAVE_AJUSTADOR,
            t1.NOMBRE_AJUSTADOR,
			t1.SINIESTRO,
			SUM(t1.MONTO_INGRESO) AS TOT_MONTO_INGRESO,
			COUNT(*) AS NUM_PAGOS
		FROM
			WORK.SINIESTROS_T2 t1
		GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			t1.SINIESTRO;
QUIT;
RUN;
     
/*Toma el primer nombre de ajustador dado la clave de ajustador tuviera más de un nombre de origen*/

proc sort data=SINIESTROS_T2_1 out=SINIESTROS_T2_1 nodupkey;
 by CVE_NOMBRE_AJUSTADOR_GR  SINIESTRO  ;
quit;

*-----------------------------------------*
* Crea las Prep Tables Motora y Resultado *
*-----------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R6_4_ENGINE) by oracle;
disconnect from oracle;
QUIT;
PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_4_ENGINE
		SELECT
            t2.CVE_NOMBRE_AJUSTADOR_GR,
            t2.NOMBRE_AJUSTADOR_GR,
            t1.FEC_REP,
			t1.SINIESTRO,
			t1.TIP_RESP_ORI,
			t1.COD_RESP,
			t2.TIPO,
			t2.COBERTURA,
			t2.TIPO_INGRESO,
			t2.MONTO_INGRESO,
            t2.FECHA_DE_OCURRIDO,
            t2.FEC_DEPOSITO,
            t1.CVE_AJUS AS AJUSTADOR,
            t2.NOMBRE_AJUSTADOR,
            INTCK('DAY', datepart(FECHA_DE_OCURRIDO), datepart(FEC_DEPOSITO)) as DIAS_OCURRE_DEP,
	        DATETIME() as BATCHDATE,
	        "&SYSUSERID." AS SYSUSERID,
            DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.SINIESTROS_T1 t1
INNER JOIN WORK.SINIESTROS_T2 t2 ON (t1.SINIESTRO = t2.SINIESTRO AND t1.CVE_AJUS=t2.CLAVE_AJUSTADOR);
QUIT;
RUN;

%put &=UMBRAL_DIAS_OCURRE_DEP;
PROC SQL;
	CREATE TABLE WORK.SINIESTROS_T3 AS
		SELECT
			*
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_4_ENGINE
		WHERE DIAS_OCURRE_DEP &UMBRAL_DIAS_OCURRE_DEP.
        ORDER BY SINIESTRO
;
QUIT;
RUN;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_4_DETAIL 
         	where trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_4_DETAIL
		SELECT
            t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.NOMBRE_AJUSTADOR_GR,
            t1.FEC_REP,
			t1.SINIESTRO,
			t1.TIP_RESP_ORI,
			t1.COD_RESP,
			t1.TIPO,
			t1.COBERTURA,
			t1.TIPO_INGRESO,
            t1.MONTO_INGRESO,
            t1.FECHA_DE_OCURRIDO,
            t1.FEC_DEPOSITO,
            t1.AJUSTADOR,
			t1.NOMBRE_AJUSTADOR,
            t1.DIAS_OCURRE_DEP,
	        DATETIME() as BATCHDATE,
	        "&SYSUSERID." AS SYSUSERID,
            DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.SINIESTROS_T3 t1;
QUIT;
RUN;

*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_4_DETAIL 
     where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

/*Obtiene el periodo actual de la tabla QCS_PREP_PREV_R6_4_DETAIL*/
data   tmp_QCS_PREP_PREV_R6_4_DETAIL;
   set  &LIBN_SAS..QCS_PREP_PREV_R6_4_DETAIL;
   if  datepart(PERIOD_BATCHDATE) =  datepart(DHMS(&FEC_FIN.,0,0,0));
run;

/*Obtiene el maximo dia de DIAS_OCURRE_DEP por cada CVE_NOMBRE_AJUSTADOR_GR*/
proc sort data =tmp_QCS_PREP_PREV_R6_4_DETAIL out=num_max_dias_cve_ajust (keep=SINIESTRO  AJUSTADOR  CVE_NOMBRE_AJUSTADOR_GR NOMBRE_AJUSTADOR_GR DIAS_OCURRE_DEP ) ;
  by CVE_NOMBRE_AJUSTADOR_GR  descending DIAS_OCURRE_DEP ;
quit;

data  SINIESTROS_T4 (drop=DIAS_OCURRE_DEP );
  retain MAX_DIAS_OCURRE_DEP 0;
   set num_max_dias_cve_ajust;
   by CVE_NOMBRE_AJUSTADOR_GR;

   if first.CVE_NOMBRE_AJUSTADOR_GR then   do;
      MAX_DIAS_OCURRE_DEP = DIAS_OCURRE_DEP;
    end;
run;
    
proc sort data =SINIESTROS_T4 nodupkey ;
  by CVE_NOMBRE_AJUSTADOR_GR siniestro;
quit;


PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R6_4_RESULT) by oracle;
disconnect from oracle;
QUIT;
PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_4_RESULT
		SELECT DISTINCT
            t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.NOMBRE_AJUSTADOR_GR, 
            t1.AJUSTADOR,
			t2.NOMBRE_AJUSTADOR,
			t2.SINIESTRO,
			t2.TIP_RESP_ORI,
			t2.COD_RESP,
			t2.TIPO,
			t2.COBERTURA,
			t2.TIPO_INGRESO,
            t3.NUM_PAGOS,
			t3.TOT_MONTO_INGRESO AS MONTO_INGRESO,
            t2.FECHA_DE_OCURRIDO,
            t2.FEC_DEPOSITO,
            t1.MAX_DIAS_OCURRE_DEP AS DIAS_OCURRE_DEP,
            t1.SINIESTRO AS SINIESTRO_INCUMP_DEPO,
	       DATETIME() as BATCHDATE,
	       "&SYSUSERID." AS SYSUSERID,
           DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE

		FROM
			WORK.SINIESTROS_T4 t1    
        INNER JOIN tmp_QCS_PREP_PREV_R6_4_DETAIL  t2 
        ON (t1.SINIESTRO = t2.SINIESTRO AND t1.CVE_NOMBRE_AJUSTADOR_GR=t2.CVE_NOMBRE_AJUSTADOR_GR 
             AND t1.MAX_DIAS_OCURRE_DEP=t2.DIAS_OCURRE_DEP  )

LEFT JOIN WORK.SINIESTROS_T2_1 t3 ON (t1.SINIESTRO = t3.SINIESTRO AND t1.AJUSTADOR=t3.CLAVE_AJUSTADOR);
QUIT;
RUN;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;