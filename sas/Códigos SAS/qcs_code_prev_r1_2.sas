******************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                               *
* FECHA: Enero 20, 2022                                                                              *
* TITULO: Creación de Prep Tables Familia 1 para la regla 1.2                                        *
* OBJETIVO: Cancelaciones (automáticas y no automáticas) por falta de pago en los últimos 12 meses   *
* ENTRADAS: COBRANZA, CONTROL_DE_AGENTES, QCS_PARAM_PREV_R1_2                                        *
* SALIDAS: QCS_PREP_PREV_R1_2_ENGINE, QCS_PREP_PREV_R1_2_DETAIL, QCS_PREP_PREV_R1_2_RESULT           *
******************************************************************************************************;
%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macro_reexpedidas_reexpedicion.sas';

*------------------------*
* Parametros de sistema  *
*------------------------*;
options compress=no;  

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

*------------------------------------------------------------------------*
* Parametro: Obtiene los dias definidos para validacion de reexpedicion  *
*------------------------------------------------------------------------*;
PROC SQL NOPRINT;
    SELECT PARAMETER_VALUE
     INTO :UMBRAL_REXP
        FROM &LIBN_SAS..QCS_PARAM_PREV_R1_2
	WHERE NAME_OF_THE_RULE='PREVENCION 1.2' AND PARAMETER_NAME='UMBRAL_REEXP';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Polizas  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_POLIZAS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_2
	WHERE NAME_OF_THE_RULE='PREVENCION 1.2' AND PARAMETER_NAME='LISTA_POLIZAS';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Agentes  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AGENTE AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_2
	WHERE NAME_OF_THE_RULE='PREVENCION 1.2' AND PARAMETER_NAME='LISTA_AGENTE';
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

*------------------------------------------------------------*
* Identifica las polizas individuales de cobranza            *
* Integra la informacion del agente unico  (Agente, Gerente) *
*------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T1_PRE as
SELECT t2.POL as POLIZA, t2.ENDO, t2.INCISO, t2.INCISOS, t2.ESTATUS_INCISO, kstrip(t2.SERIE) as SERIE ,
         t2.DESDE, t2.HASTA, t2.FEC_EMI, t2.AGENTE length=10 format=$10., t2.ASEG, t2.ASEGURADO, t2.PRIMA_TOTAL, 
         t2.PMA_PAGADA, t2.PMA_PENDIENTE, t2.USUARIO_EMI, t2.DESCRIP, t2.FORMA_PAGO, 
         t2.SINIESTROS, t2.USO, t2.NOM_OFIC length=50 format=$50., t2.REMESA, 
         t2.SUBRAMO, t2.BATCHDATE
    FROM   &LIBN_ORA..COBRANZA t2   
  inner join  COBRANZA_PII t1
    ON (t1.POL=t2.POL);
QUIT;

proc sort data=COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
run;

data COBRANZA_T1_PRE;
set COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
if last.SUBRAMO;
drop SUBRAMO BATCHDATE;
run;

/*************************/
Proc sql;
create table Work.COBRANZA_T1 as
SELECT t2.*,
        t3.CVE_AGTE,
        t3.NOM_AGTE,
        t3.CLAVE_UNICA_AGENTE,
        t3.GERENTE
from COBRANZA_T1_PRE AS t2
  
 /*Incluye la información de agente unico*/
  LEFT JOIN CONTROL_DE_AGENTES t3 
    ON (t2.AGENTE=t3.CVE_AGTE)
where missing(SERIE) = 0   
 /*Descarta las polizas contenidas en la lista de exclusion*/
and POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
/*Descarta los agentes contenidos en la lista de exclusion*/
and AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE);
Quit;


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
         GERENTE=GERENTE_TMP   AGENTE = AGENTE_TMP
   ) );
   length CLAVE_UNICA_AGENTE_ORG $200.;
   length  CVE_AGTE  $5.;
   length  AGENTE   $10; 
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

*------------------------------------------------------------*
* Toma la emision de las polizas para validar reexpediciones *
*------------------------------------------------------------*;
data COBRANZA_T2;
   set COBRANZA_T1;
 key_unique = _N_;
  if  kstrip(ENDO) ='000000';
run;
*----------------- INICIO: <<IDENTIFICA REEXPEDICIONES>> -----------------*;
    
%obtiene_reexpedida_reexpedicion(
   tabla_pol_indiv_endo_0=COBRANZA_T2, /*Tiene solo movimientos con endoso ='000000' */
   tabla_pol_indiv_all_endo=COBRANZA_T1_PRE,  /*Tiene  todos los movimientos */
   UMBRAL_REXP=&UMBRAL_REXP.   /*parámetro de umbral, Ajustado dinamicamete */
); 

*------------------------------------------------------------------------------------------------------*
* Termina la aplicacion de las reglas para seleccionar la póliza con reexpedición                      *
*------------------------------------------------------------------------------------------------------*;
*-------------------------------------------------------------------*
* Obtiene los movimientos de las polizas que tienen REEXPEDICION *
*-------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T9 AS
	SELECT *
    FROM WORK.COBRANZA_T1
	WHERE
		POLIZA IN (SELECT DISTINCT POLIZA FROM _OUT_REEXPEDIDA_REEXPEDICION WHERE FLG_REEXPEDICION_V = 0);
QUIT;
RUN;

*-----------------------------------------------------------------*
* Obtiene el ultimo movimiento de la POLIZA descartanto ENDOSOS 3 *
*-----------------------------------------------------------------*;
PROC SORT DATA=Work.COBRANZA_T9 (WHERE=(SUBSTR(ENDO,1,1) ne '3')); 
 BY POLIZA FEC_EMI; 
RUN;	

DATA Work.COBRANZA_T10;
 SET Work.COBRANZA_T9;
 BY POLIZA FEC_EMI;
 IF LAST.POLIZA THEN OUTPUT;
RUN;

*----------------------------------------------------------------------------------*
* Obtiene el % promedio de siniestralidad año anterior y en curso por agente unico *
*----------------------------------------------------------------------------------*;
Data WORK.CONTROL_DE_AGENTES_porc;
 Set &LIBN_ORA..CONTROL_DE_AGENTES
(keep=CLAVE_UNICA_AGENTE 
      CVE_AGENTE 
      PORC_SIN_MES_ANT
      PORC_SIN_A_DIC_ANIO_ANT 
      PORC_SIN_MES_ANIO_ANT);
Run;

Data WORK.CONTROL_DE_AGENTES_porc;
modify WORK.CONTROL_DE_AGENTES_porc;
array VARS{*} PORC_SIN_MES_ANT PORC_SIN_A_DIC_ANIO_ANT PORC_SIN_MES_ANIO_ANT;
do i = 1 to dim(VARS);
/*los valores menores o iguales a cero se reemplazan por nulos*/
   if VARS{i} <= 0 then call missing(VARS{i});
end;
Run;

Proc sql;
create table WORK.SINIESTRALIDAD as
	select
		t1.CLAVE_UNICA_AGENTE,
        mean(t1.PORC_SIN_MES_ANT) AS PORC_SIN_MES_ANT,
        mean(t1.PORC_SIN_MES_ANIO_ANT) as PORC_SIN_ANIO_CURSO,
		mean(t1.PORC_SIN_A_DIC_ANIO_ANT) AS PORC_SIN_ANIO_ANTERIOR
	from
		WORK.CONTROL_DE_AGENTES_porc t1
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;
*-----------------------------*
* Creacion de la Tabla Motora *
*-----------------------------*;
DATA WORK.COBRANZA_T11
(keep=POLIZA ENDO INCISO INCISOS DESDE HASTA FEC_EMI CVE_AGTE AGENTE NOM_AGTE CLAVE_UNICA_AGENTE
      SERIE ESTATUS_INCISO REMESA USUARIO_EMI CANCELACION_AUTOMATICA ESTATUS);
 SET Work.COBRANZA_T10;
 
 if ESTATUS_INCISO='0001-CANCELADO' and USUARIO_EMI='CANCELA-AUT' 
 then CANCELACION_AUTOMATICA='SI'; 
 else CANCELACION_AUTOMATICA='NO';

 if ESTATUS_INCISO='0001-CANCELADO' 
 then ESTATUS='CANCELADO';
 else ESTATUS='VIGENTE';
RUN;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_2_ENGINE) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
create table QCS_PREP_PREV_R1_2_ENGINE_pre as
    SELECT t1.*,
		   t2.PORC_SIN_ANIO_CURSO,
           t2.PORC_SIN_ANIO_ANTERIOR,
       	   DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID length=10,
           DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
	FROM WORK.COBRANZA_T11 t1
	LEFT JOIN WORK.SINIESTRALIDAD t2 ON
    t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE;
QUIT;
RUN;
proc append data=QCS_PREP_PREV_R1_2_ENGINE_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE;
run;
*------------------------------*
* Creacion de la Tabla Detalle *
*------------------------------*;

*----------------------------------------------*
* Completa la marca de polizas de reexpedicion *
*----------------------------------------------*;
proc sort data=_OUT_REEXPEDIDA_REEXPEDICION (keep =   POLIZA
                 SERIE
                 FLG_REEXPEDICION_V
                 CON_PMA_PAGADA)    out=FLG_REEXPEDICION_V4
 nodupkey;
   by               POLIZA
                    SERIE;
quit; 



PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_2_DETAIL) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
create table QCS_PREP_PREV_R1_2_DETAIL_pre as 
	SELECT t1.POLIZA,
        t1.ENDO,
		t1.INCISO,
		t1.FEC_EMI,
		t1.DESDE,
		t1.HASTA,
		t1.ASEG,
		t1.AGENTE,
		t1.ESTATUS_INCISO,
		t1.SERIE,
        t1.PRIMA_TOTAL,
		t1.PMA_PAGADA,
		t1.PMA_PENDIENTE,
        t1.USUARIO_EMI,
        t1.DESCRIP,
        t1.FORMA_PAGO,
        t1.SINIESTROS,
        t1.USO,
        t1.NOM_OFIC,
/*
        t2.CLAVE_UNICA_AGENTE,
        t2.GERENTE,
        t2.NOM_AGTE,
*/
        t1.CLAVE_UNICA_AGENTE,
        t1.GERENTE,
        t1.NOM_AGTE,

        t3.CANCELACION_AUTOMATICA, 
        t3.ESTATUS,
        CASE WHEN t4.CON_PMA_PAGADA =. THEN 0 ELSE t4.CON_PMA_PAGADA END AS FLG_PMA_PAGADA,
        CASE WHEN t4.FLG_REEXPEDICION_V =. THEN . ELSE t4.FLG_REEXPEDICION_V END AS FLG_REEXPEDICION,
       	DATETIME() as BATCHDATE,
        "&SYSUSERID." AS SYSUSERID length=10,
        DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
    FROM COBRANZA_T1 AS t1 /* COBRANZA_T1 Tiene todos los movimientos del periodo*/

  /*Incluye la información de agente unico*/
    /*LEFT JOIN CONTROL_DE_AGENTES AS t2 ON t1.AGENTE=t2.CVE_AGTE  */

  /*Incluye la información de agente unico solo de la póliza con reexpedición*/
    LEFT JOIN (SELECT DISTINCT POLIZA, ENDO, CANCELACION_AUTOMATICA, ESTATUS
               FROM &LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE) AS t3 ON t1.POLIZA=t3.POLIZA AND t1.ENDO=t3.ENDO
   
  /*Marca las polizas que son reexpedidas*/   
    LEFT JOIN (SELECT  
                 POLIZA, 
                 SERIE,
                 FLG_REEXPEDICION_V,
                 CON_PMA_PAGADA
               FROM FLG_REEXPEDICION_V4) AS t4
    ON t1.POLIZA=t4.POLIZA AND 
       t1.SERIE=t4.SERIE 
  
	WHERE
		/*t1.SERIE IN (SELECT DISTINCT SERIE FROM &LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE) */
      t1.POLIZA IN (SELECT DISTINCT POLIZA FROM &LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE) 
	/*Descarta las polizas contenidas en la lista de exclusion*/
	and t1.POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
	/*Descarta los agentes contenidos en la lista de exclusion*/
	and t1.AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE)
;
QUIT;
RUN;

proc append data=QCS_PREP_PREV_R1_2_DETAIL_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_2_DETAIL force;
run;
   
*-------------------------------------------------------*
* Obtiene el total de polizas vigentes por agente unico *
*-------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T12 AS
	SELECT DISTINCT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS CNT_POL_VIG
	FROM
		/*WORK.COBRANZA_T1 t1  */
       &LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE t1
	WHERE
		upcase(t1.ESTATUS_INCISO) contains 'VIGENTE' and  missing(t1.CLAVE_UNICA_AGENTE) = 0
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
*-------------------------------------------------------------------------------------*
* Obtiene el total de polizas canceladas y de cancelacion automatica por agente unico *
*-------------------------------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T13 AS
	SELECT DISTINCT
		t1.CLAVE_UNICA_AGENTE,
        COUNT(DISTINCT(t1.POLIZA)) AS CNT_POL_CAN_AUT
	FROM
		&LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE t1
    WHERE
		t1.CANCELACION_AUTOMATICA='SI' and  missing(t1.CLAVE_UNICA_AGENTE) = 0
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T14 AS
	SELECT DISTINCT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS CNT_POL_CANCELADA
	FROM
		&LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE t1
    WHERE
		upcase(t1.ESTATUS)='CANCELADO' and missing(t1.CLAVE_UNICA_AGENTE) = 0
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
*-------------------------------------------*
* Creacion de la Tabla de Resultado Entidad *
*-------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T15 AS
	SELECT DISTINCT
		t1.CLAVE_UNICA_AGENTE,
        mean(t1.PORC_SIN_ANIO_ANTERIOR) as PORC_SIN_ANIO_ANTERIOR, 
        mean(t1.PORC_SIN_ANIO_CURSO) as PORC_SIN_ANIO_CURSO
	FROM
		&LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE t1
         where missing(t1.CLAVE_UNICA_AGENTE) = 0 
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
  
*-------------------------------------------------------------------------------------------------*
* Obtiene el total de polizas por agente unico Tomando todas las polizas sin importar el estatus  *
*-------------------------------------------------------------------------------------------------*;

Proc sql;  
create table WORK.COBRANZA_T16 as
	select
		t1.CLAVE_UNICA_AGENTE,
		count(distinct(t1.POLIZA)) AS CNT_POL_AGTE
	from
		WORK.COBRANZA_T1 t1
	where
		     kstrip(ENDO) = '000000'  and missing(CLAVE_UNICA_AGENTE) =0
           and  poliza in (select  distinct poliza from _OUT_REEXPEDIDA_REEXPEDICION 
                            where FLG_REEXPEDICION_V = 0     )
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;



*---------------------------------*
* Creacion de la Tabla Resultados *
*---------------------------------*;
  
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_2_RESULT) by oracle;
disconnect from oracle;
QUIT;
Proc sql;
    create table QCS_PREP_PREV_R1_2_RESULT_pre as
	select
		t1.CLAVE_UNICA_AGENTE,
        t5.CNT_POL_AGTE,
		case when t3.CNT_POL_CAN_AUT is not missing then t3.CNT_POL_CAN_AUT else 0 end as CNT_POL_CAN_AUT,
        case when t4.CNT_POL_CANCELADA is not missing then t4.CNT_POL_CANCELADA else 0 end as CNT_POL_CANCELADA,
        t2.CNT_POL_VIG,
        divide(calculated CNT_POL_CAN_AUT,sum(calculated CNT_POL_CANCELADA,t2.CNT_POL_VIG)) as PCT_POL_CAN_AUT,
        divide(calculated CNT_POL_CANCELADA,sum(calculated CNT_POL_CANCELADA,t2.CNT_POL_VIG)) as PCT_POL_CANCELADA,
        t1.PORC_SIN_ANIO_ANTERIOR, 
        t1.PORC_SIN_ANIO_CURSO,
       	DATETIME() as BATCHDATE,
        "&SYSUSERID." AS SYSUSERID length=10,
        DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
	from Work.COBRANZA_T15 t1
      
   /*  Obtiene el total de polizas vigentes por agente unico */
	left join WORK.COBRANZA_T12 t2 on
    t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE

   /* Obtiene el total de polizas canceladas y de cancelacion automatica por agente unico */
	left join WORK.COBRANZA_T13 t3 on
    t1.CLAVE_UNICA_AGENTE=t3.CLAVE_UNICA_AGENTE

    /*Obtiene pólizas canceladas*/
    left join WORK.COBRANZA_T14 t4 on
    t1.CLAVE_UNICA_AGENTE=t4.CLAVE_UNICA_AGENTE

   /*  Obtiene el total de polizas por agente unico */
    left join WORK.COBRANZA_T16 t5 on
    t1.CLAVE_UNICA_AGENTE=t5.CLAVE_UNICA_AGENTE;
Quit;
  
proc append data=QCS_PREP_PREV_R1_2_RESULT_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_2_RESULT;
run;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;