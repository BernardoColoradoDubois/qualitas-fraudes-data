******************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                               *
* FECHA: Septiembre 1, 2021                                                                          *
* TITULO: Creación de Prep-Table Familia 1 para regla 1.5                                            *
* OBJETIVO: Póliza de RC obligatoria o de ramos no cancelables sin pago de prima mayor a 45 días     *
* ENTRADAS: COBRANZA, CONTROL_DE_AGENTES                                                             *
* SALIDAS: QCS_PREP_PREV_R1_5_ENGINE, QCS_PREP_PREV_R1_5_DETAIL, QCS_PREP_PREV_R1_5_RESULT           *
******************************************************************************************************;

%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macro_reexpedidas_reexpedicion.sas';
*------------------------*
* Parametros de sistema  *
*------------------------*;
Options compress=NO;
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

/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

*-------------------------------------------------*
* Parametro: Fecha de actualizacion de los datos  *
*-------------------------------------------------*;
%LET FECHA_INFORMACION = datetime();
%put &=FECHA_INFORMACION;
*---------------------------------------------*
* Parametro: Dias de antiguedad de la poliza  *
*---------------------------------------------*;
%LET DIAS_DE_ANTIGUEDAD = 45;
*-------------------------------------------*
* Obtiene la lista de exclusion de Polizas  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_POLIZAS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_5
	WHERE NAME_OF_THE_RULE='PREVENCION 1.5' AND PARAMETER_NAME='LISTA_POLIZAS';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Agentes  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AGENTE AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_5
	WHERE NAME_OF_THE_RULE='PREVENCION 1.5' AND PARAMETER_NAME='LISTA_AGENTE';
QUIT;
RUN;
*-------------------------------*
* Obtiene la lista de subramos  *
*-------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_SUBRAMOS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_5
	WHERE NAME_OF_THE_RULE='PREVENCION 1.5' AND PARAMETER_NAME='LISTA_SUBRAMOS';
QUIT;
RUN;
*----------------------------------------------*
* Obtiene el valor para el campo de validacion *
*----------------------------------------------*;
PROC SQL NOPRINT;
    SELECT PARAMETER_VALUE
     INTO :CAMPO_VALIDACION
        FROM &LIBN_SAS..QCS_PARAM_PREV_R1_5
	WHERE NAME_OF_THE_RULE='PREVENCION 1.5' AND PARAMETER_NAME='CAMPO_VALIDACION';
QUIT;
RUN;

%put &=CAMPO_VALIDACION;
*------------------------------------------------------*
* Obtiene la lista de valores del campo de validacion  *
*------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_VALIDACION AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_5
	WHERE NAME_OF_THE_RULE='PREVENCION 1.5' AND PARAMETER_NAME='LISTA_VALIDACION';
QUIT;
RUN;

data poliza_flotilla;
	set &LIBN_ORA..COBRANZA(keep=POL INCISO);
	length INCISO2 $10.;
	INCISO2=compress(compress(INCISO), "0123456789", 'kis');
	INCISO_num=input(compress(INCISO2, , "kw"), best32.);

	if INCISO_num >  1 then
		do;
			output poliza_flotilla;
		end;
run;

proc  sort data=poliza_flotilla(keep= POL) out=pol_flotilla_unique nodupkey;
   by POL;
quit;

proc sql;
create table COBRANZA_PII as
SELECT DISTINCT 
                  POL, 
                  INCISO, 
                  INCISOS 
              FROM &LIBN_ORA..COBRANZA (keep=POL ENDO INCISO INCISOS AGENTE FEC_EMI)
              WHERE 
					datepart(FEC_EMI) between  &fec_inicio. and &fec_fin. AND
                    ENDO='000000' AND 
                    INCISO='0001' AND 
                    INCISOS= 1 
                 /*Elimina falsos positivos, identificados como poliza individuales */
                    and   POL not in(select POL from  pol_flotilla_unique);      
quit;

proc sort data=COBRANZA_PII nodupkey;
 by POL;
quit;

/*Obtiene datos del agente*/
PROC SQL;
CREATE TABLE CONTROL_DE_AGENTES AS
SELECT DISTINCT
                   CVE_AGENTE AS CVE_AGTE, 
                   AGENTE_NOM AS NOM_AGTE, 
                   CLAVE_UNICA_AGENTE,
                   GERENTE
               FROM &LIBN_ORA..CONTROL_DE_AGENTES;
QUIT;

proc sort data=CONTROL_DE_AGENTES nodupkey;
  by CLAVE_UNICA_AGENTE CVE_AGTE ;
quit;

*-------------------------------------------------------------------------*
* Identifica las polizas individuales de cobranza,                        *
* Integra la informacion del agente unico  (Agente, Gerente)              *
*-------------------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T1_PRE as
 SELECT t2.POL AS POLIZA, t2.ENDO, t2.INCISO, t2.INCISOS, t2.ESTATUS, t2.ESTATUS_INCISO, 
         t2.SERIE, t2.DESDE, t2.HASTA, t2.FEC_EMI, t2.AGENTE, t2.ASEG, t2.PRIMA_TOTAL, t2.PMA_PAGADA, 
         t2.PMA_PENDIENTE, t2.USUARIO_EMI, t2.DESCRIP, t2.FORMA_PAGO, t2.SINIESTROS, t2.USO, t2.NOM_OFIC, 
         t2.REMESA, t2.COBERTURA, t2.TARIFA, t2.SUBRAMO, 
         t2.BATCHDATE
    FROM COBRANZA_PII t1
  /*Obtiene las polizas individuales*/
   inner JOIN &LIBN_ORA..COBRANZA t2 ON t1.POL=t2.POL;
quit;

proc sort data=COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
run;

data COBRANZA_T1_PRE;
set COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
if last.SUBRAMO;
drop BATCHDATE;
run;

/**************/
Proc sql;
create table Work.COBRANZA_T1 as
 SELECT t2.*,
         t3.CVE_AGTE, t3.NOM_AGTE, t3.CLAVE_UNICA_AGENTE, t3.GERENTE
from COBRANZA_T1_PRE t2
  /*Incluye la información de agente unico*/
  LEFT JOIN CONTROL_DE_AGENTES t3 
  ON (t2.AGENTE=t3.CVE_AGTE)
where /*Descarta las polizas contenidas en la lista de exclusion*/
POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
/*Descarta los agentes contenidos en la lista de exclusion*/
and AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE);
Quit;

data COBRANZA_T1;
set COBRANZA_T1;
DIAS_ANTIGUEDAD=INTCK('DTDAY', FEC_EMI, &FECHA_INFORMACION.);
run;


/*Inicia proceso de asignación de agente */
/*Asigna a la póliza la clave de agente con endoso =000000
 Regla de cve de agente= si la póliza tiene mas dos claves unica de agente, se tomara para la póliza 
 la clave unica de agente con endoso=000000 
 */
  /*Obtiene la clave unica de agente con endodoso=000000 */
proc sql;    
 create table cve_agente_endo_0 as  
  select poliza,
         endo,
         FEC_EMI,
        CVE_AGTE as CVE_AGTE_2,
        NOM_AGTE as NOM_AGTE_2,
        CLAVE_UNICA_AGENTE as CLAVE_UNICA_AGENTE_2,
        GERENTE as  GERENTE_2
 from  COBRANZA_T1
   where  ENDO='000000';
quit;
  
proc sort data=cve_agente_endo_0;
  by poliza endo descending FEC_EMI;
quit;

proc sort data=cve_agente_endo_0 out=cve_agente_endo_0 nodupkey;
  by poliza ;
quit;
    
proc sql;
 create table COBRANZA_T1_0  as
   select  b.*,
      CVE_AGTE_2,
      NOM_AGTE_2,
      CLAVE_UNICA_AGENTE_2,
      GERENTE_2
  from  COBRANZA_T1 b
   left join  cve_agente_endo_0 c
    on b.poliza=c.poliza;
quit;   
          
data COBRANZA_T1_1 (drop=
   AGENTE_TMP
   CVE_AGTE_TMP 
   NOM_AGTE_TMP
   CLAVE_UNICA_AGENTE_TMP
   GERENTE_TMP
   CVE_AGTE_2
   NOM_AGTE_2
   CLAVE_UNICA_AGENTE_2
   GERENTE_2
  );
 set  COBRANZA_T1_0 (rename= (
         CVE_AGTE=CVE_AGTE_TMP
         NOM_AGTE=NOM_AGTE_TMP 
         CLAVE_UNICA_AGENTE=CLAVE_UNICA_AGENTE_TMP
         GERENTE=GERENTE_TMP
         AGENTE = AGENTE_TMP
   ) );
   length CLAVE_UNICA_AGENTE_ORG $200.;
   length  CVE_AGTE  $5.;
   length  AGENTE   $5.; 
   length  NOM_AGTE   $100.;
   length  CLAVE_UNICA_AGENTE  $200.;
   length  GERENTE $150.;

    CLAVE_UNICA_AGENTE_ORG = CLAVE_UNICA_AGENTE_TMP;
  
   if  missing(kstrip(CLAVE_UNICA_AGENTE_2)) = 0  then do;
     AGENTE = CVE_AGTE_2;
     CVE_AGTE = CVE_AGTE_2;
     NOM_AGTE = NOM_AGTE_2;
     CLAVE_UNICA_AGENTE = CLAVE_UNICA_AGENTE_2;
     GERENTE =GERENTE_2;
   end;  
   else if  missing(kstrip(CLAVE_UNICA_AGENTE_2)) = 1 then do;
     AGENTE = AGENTE_TMP;
     CVE_AGTE = CVE_AGTE_TMP;
     NOM_AGTE = NOM_AGTE_TMP;
     CLAVE_UNICA_AGENTE = CLAVE_UNICA_AGENTE_TMP;
     GERENTE =GERENTE_TMP;
   end;
run;

data  COBRANZA_T1;
  set  COBRANZA_T1_1;
run;

/*Termina proceso de asignación de agente */


*--------------------------------------------------*
* Descarta las polizas con movimientos de endoso 3 *
*--------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T2 AS
	SELECT * 
	FROM WORK.COBRANZA_T1
	WHERE 
        SUBSTR(STRIP(ENDO),1,1) ne '3'
;
QUIT;
RUN;
*-------------------------------------------------------------*
* Identifica las polizas sin pago de prima en sus movimientos *
*-------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T3 AS
	SELECT t1.*, t2.POLIZA_SIN_PAGO
   	FROM WORK.COBRANZA_T2 AS t1
	LEFT JOIN
			(SELECT POLIZA, 
	           CASE WHEN SUM(PMA_PAGADA) = 0 
                    THEN 1 ELSE 0
	           END 
				AS POLIZA_SIN_PAGO
			   FROM WORK.COBRANZA_T2
			  GROUP BY POLIZA) AS t2 
    ON t1.POLIZA = t2.POLIZA
	ORDER BY t1.POLIZA, t1.DESDE, t1.FEC_EMI
;
QUIT;
RUN;
*-----------------------------------------------------------------*
* Obtiene las polizas de RC obligatoria o de Ramos No Cancelables *
*-----------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T4 AS
	SELECT *
	FROM WORK.COBRANZA_T3
	WHERE 
        &CAMPO_VALIDACION. in (select distinct PARAMETER_VALUE from Work.LISTA_VALIDACION) or 
        SUBRAMO in (select distinct PARAMETER_VALUE from Work.LISTA_SUBRAMOS)
;
QUIT;
RUN;
*------------------------------------------------------*
* Identifica las polizas con mas de 45 dias de emision *
*------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T5 AS
	SELECT * 
	FROM WORK.COBRANZA_T4
	WHERE 
        DIAS_ANTIGUEDAD > &DIAS_DE_ANTIGUEDAD.
;
QUIT;
RUN;
*----------------------------------------------*
* Obtiene el total de polizas por agente unico *
*----------------------------------------------*;
    
*----------------- INICIO: <<IDENTIFICA REEXPEDICIONES>> -----------------*;
data input_COBRANZA_T1;
    set  COBRANZA_T1;
  key_unique = _N_;
     if  kstrip(endo)='000000' and  missing(serie) = 0;
run;

data input_COBRANZA_T1_PRE;
   set COBRANZA_T1_PRE;
run;
  
%obtiene_reexpedida_reexpedicion(
   tabla_pol_indiv_endo_0=input_COBRANZA_T1, /*Tiene solo movimientos con endoso ='000000' */
   tabla_pol_indiv_all_endo=input_COBRANZA_T1_PRE,  /*Tiene  todos los movimientos */
   UMBRAL_REXP=0      /*parámetro de umbral, el valor de cero indica que traera todas las pólizas con reexpedición */
); 
*----------------- TERMINA: <<IDENTIFICA REEXPEDICIONES>> -----------------*;

PROC SQL;
CREATE TABLE WORK.COBRANZA_T6 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS TOTAL_POL_AGTE
	FROM
		WORK.COBRANZA_T1 t1
	WHERE t1.ENDO ='000000' and missing(t1.CLAVE_UNICA_AGENTE ) =0 and missing(serie) = 0
    and  t1.poliza in (select  distinct poliza from _OUT_REEXPEDIDA_REEXPEDICION 
                            where FLG_REEXPEDICION_V = 0     )
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
RUN;

*-------------------------------------------------------------------*
* Obtiene el total de polizas ramos no cancelables por agente unico *
*-------------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T7 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS POL_RAMOS_NO_CANC
	FROM
		WORK.COBRANZA_T4 t1
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*---------------------------------------------------------------------------------*
* Obtiene el total de polizas canceladas de ramos no cancelables por agente unico *
*---------------------------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T8 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS POL_CANC_RAMOS_NO_CANC
	FROM
		WORK.COBRANZA_T4 t1
	WHERE
		upcase(T1.ESTATUS_INCISO) contains 'CANCELADO'
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*------------------------------------------------------------------*
* Obtiene el total de polizas canceladas sin pago por agente unico *
*------------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T9 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS POL_CANC_SINPAGO
	FROM
		WORK.COBRANZA_T4 t1
	WHERE
		upcase(T1.ESTATUS_INCISO) contains 'CANCELADO' AND
        T1.POLIZA_SIN_PAGO = 1 
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*-------------------------------------------------------------------------------*
* Obtiene el total de polizas vigentes de ramos no cancelables por agente unico *
*-------------------------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T10 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS POL_VIG_RAMOS_NO_CANC
	FROM
		WORK.COBRANZA_T4 t1
	WHERE
		upcase(T1.ESTATUS_INCISO) contains 'VIGENTE'
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*-----------------------------------------------------------------------------------------------------------*
* Obtiene el total de polizas vigentes de ramos no cancelables sin pago superior a 45 dias por agente unico *
*-----------------------------------------------------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T11 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS POL_VIG_RAMOS_NO_CANC_SINPAGO
	FROM
		WORK.COBRANZA_T5 t1
	WHERE
		 UPCASE(T1.ESTATUS_INCISO) contains 'VIGENTE' AND
		T1.POLIZA_SIN_PAGO = 1
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*-------------------------------------------*
* Creacion de la Tabla de Resultado Entidad *
*-------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.QCS_PREP_PREV_R1_5_RESULT AS
	SELECT DISTINCT
		t1.CLAVE_UNICA_AGENTE,
		t2.TOTAL_POL_AGTE,
		t1.POL_RAMOS_NO_CANC,
		t3.POL_CANC_RAMOS_NO_CANC,
        t4.POL_CANC_SINPAGO,
        t5.POL_VIG_RAMOS_NO_CANC,
        t6.POL_VIG_RAMOS_NO_CANC_SINPAGO,
        DIVIDE(t3.POL_CANC_RAMOS_NO_CANC, t1.POL_RAMOS_NO_CANC) as PCT_POL_CANCELADAS,
        DIVIDE(t6.POL_VIG_RAMOS_NO_CANC_SINPAGO, t1.POL_RAMOS_NO_CANC) as PCT_POL_VIG_RAMOS_NO_CANC
	FROM
		WORK.COBRANZA_T7 t1
	LEFT JOIN WORK.COBRANZA_T6 t2 ON t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE
	LEFT JOIN WORK.COBRANZA_T8 t3 ON t1.CLAVE_UNICA_AGENTE=t3.CLAVE_UNICA_AGENTE
	LEFT JOIN WORK.COBRANZA_T9 t4 ON t1.CLAVE_UNICA_AGENTE=t4.CLAVE_UNICA_AGENTE
	LEFT JOIN WORK.COBRANZA_T10 t5 ON t1.CLAVE_UNICA_AGENTE=t5.CLAVE_UNICA_AGENTE
	LEFT JOIN WORK.COBRANZA_T11 t6 ON t1.CLAVE_UNICA_AGENTE=t6.CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*--------------------------------------*
* Reemplaza los valores nulos por cero *
*--------------------------------------*;
DATA WORK.QCS_PREP_PREV_R1_5_RESULT;
 SET WORK.QCS_PREP_PREV_R1_5_RESULT;
    ARRAY NUM_ARRAY _NUMERIC_;
	DO OVER NUM_ARRAY;
       IF MISSING(NUM_ARRAY) THEN NUM_ARRAY = 0;
	END;
RUN;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_5_RESULT) by oracle;
disconnect from oracle;
QUIT;
PROC SQL;
create table QCS_PREP_PREV_R1_5_RESULT_pre as
SELECT CLAVE_UNICA_AGENTE,
	TOTAL_POL_AGTE,
	POL_RAMOS_NO_CANC,
	POL_CANC_RAMOS_NO_CANC,
    POL_CANC_SINPAGO,
    POL_VIG_RAMOS_NO_CANC,
    POL_VIG_RAMOS_NO_CANC_SINPAGO,
    PCT_POL_CANCELADAS,
    PCT_POL_VIG_RAMOS_NO_CANC,
   	DATETIME() as BATCHDATE,
    "&SYSUSERID." AS SYSUSERID length=10,
    DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
 FROM WORK.QCS_PREP_PREV_R1_5_RESULT;
QUIT;
RUN;

proc append data=QCS_PREP_PREV_R1_5_RESULT_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_5_RESULT;
run;
*-----------------------------------------*
* Genera las Prep Tables Motora y Detalle *
*-----------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_5_ENGINE) by oracle;
disconnect from oracle;
QUIT;
PROC SQL;
create table QCS_PREP_PREV_R1_5_ENGINE_pre as
    SELECT POLIZA,
           ENDO,
           INCISOS,
           DESDE,
           HASTA,
           FEC_EMI,
           AGENTE,
           COBERTURA,
           TARIFA,
           SUBRAMO,
           PMA_PAGADA,
           CLAVE_UNICA_AGENTE,
           NOM_AGTE,
           ESTATUS_INCISO,
           DIAS_ANTIGUEDAD,
           POLIZA_SIN_PAGO,
       	   DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID length=10,
           DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
      FROM Work.COBRANZA_T4;
QUIT;
RUN;

proc append data=QCS_PREP_PREV_R1_5_ENGINE_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_5_ENGINE;
run;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_5_DETAIL) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
create table QCS_PREP_PREV_R1_5_DETAIL_pre as
    SELECT POLIZA,
           INCISO,
           ENDO,
           SERIE,
           FEC_EMI,
           DESDE,
           HASTA,
           ASEG,
           AGENTE,
           ESTATUS_INCISO,
           PRIMA_TOTAL,
           PMA_PAGADA,
           PMA_PENDIENTE,
           USUARIO_EMI,
           DESCRIP,
           FORMA_PAGO,
           SINIESTROS,
           USO,
           NOM_OFIC,
           GERENTE,
           NOM_AGTE,
           CLAVE_UNICA_AGENTE,
           COBERTURA,
           TARIFA,
           SUBRAMO,
           DIAS_ANTIGUEDAD,
           POLIZA_SIN_PAGO,
       	   DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID length=10,
           DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE       
      FROM Work.COBRANZA_T4;
QUIT;
RUN;

proc append data=QCS_PREP_PREV_R1_5_DETAIL_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_5_DETAIL;
run;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;