**************************************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                                                   *
* FECHA: Septiembre 14, 2021                                                                                             *
* TITULO: Creación de Prep-Table Familia 1 para regla 1.4                                                                *
* OBJETIVO: Póliza con cambio de forma de pago y con cancelación automática                                              *
* ENTRADAS: COBRANZA, CONTROL_DE_AGENTES                                                                                 *
* SALIDAS: QCS_PREP_PREV_R1_4_ENGINE, QCS_PREP_PREV_R1_4_DETAIL, QCS_PREP_PREV_R1_4_RESULT                               *
**************************************************************************************************************************;

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

*-------------------------------------------*
* Obtiene la lista de exclusion de Polizas  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_POLIZAS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_4
	WHERE NAME_OF_THE_RULE='PREVENCION 1.4' AND PARAMETER_NAME='LISTA_POLIZAS';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Agentes  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AGENTE AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_4
	WHERE NAME_OF_THE_RULE='PREVENCION 1.4' AND PARAMETER_NAME='LISTA_AGENTE';
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

*-------------------------------------------------------------*
* Identifica las polizas individuales de cobranza,            *
* Integra la informacion del agente unico  (Agente, Gerente)  *
*-------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T1_PRE as
SELECT t1.POL AS POLIZA, t2.ENDO AS ENDOSO, t2.INCISO, t2.INCISOS, t2.SERIE, t2.DESDE, t2.HASTA, t2.FEC_EMI, t2.ESTATUS,
         t2.PMA_PAGADA, t2.REMESA, t2.USUARIO_EMI, t2.RECIBOS, t2.RECIBOS_PAGADOS, t2.PRIMA_TOTAL, t2.PMA_PENDIENTE, t2.SUBSEC,
         t2.PRIMER_REC, t2.ESTATUS_INCISO, t2.AGENTE, t2.ASEG, t2.DESCRIP, t2.FORMA_PAGO, t2.SINIESTROS, t2.USO, t2.NOM_OFIC, 
         t2.SUBRAMO, t2.BATCHDATE
    FROM COBRANZA_PII t1
  /*Incluye la información de polizas individuales*/
  inner JOIN &LIBN_ORA..COBRANZA t2 
  on (t1.POL=t2.POL);
quit;

proc sort data=COBRANZA_T1_PRE;
by POLIZA ENDOSO INCISO SUBRAMO BATCHDATE;
run;

data COBRANZA_T1_PRE;
set COBRANZA_T1_PRE;
by POLIZA ENDOSO INCISO SUBRAMO BATCHDATE;
if last.SUBRAMO;
drop SUBRAMO BATCHDATE;
run;

data COBRANZA_T1_PRE;
  set COBRANZA_T1_PRE;
   row_unique=_N_;
run;

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
and AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE)
;
Quit;

/*Inicia proceso de asignación de agente */
/*Asigna a la póliza la clave de agente con endoso =000000
 Regla de cve de agente= si la póliza tiene mas dos claves unica de agente, se tomara para la póliza 
 la clave unica de agente con endoso=000000 
 */
  /*Obtiene la clave unica de agente con endodoso=000000 */
proc sql;    
 create table cve_agente_endo_0 as  
  select poliza,
         ENDOSO,
         FEC_EMI,
        CVE_AGTE as CVE_AGTE_2,
        NOM_AGTE as NOM_AGTE_2,
        CLAVE_UNICA_AGENTE as CLAVE_UNICA_AGENTE_2,
        GERENTE as  GERENTE_2
 from  COBRANZA_T1
   where  ENDOSO='000000';
quit;
  
proc sort data=cve_agente_endo_0;
  by poliza ENDOSO descending FEC_EMI;
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


*---------------------------------------------------*
* Identifica las polizas con estatus F/R y endoso 1 *
*---------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T2 as
SELECT DISTINCT 
       POLIZA 
	FROM COBRANZA_T1_PRE /*Tabla que contiene todos los movimientos del periodo seleccionado sin exclusión*/
	WHERE 
         ( 
            upcase(kstrip(ESTATUS))='F/R'  OR  /*Condición histórica */
           /* upcase(kstrip(ESTATUS))=upcase('F') OR  se omite ya que es igual a la condición ESTATUS_INCISO='F R'*/
            upcase(kstrip(ESTATUS_INCISO)) = upcase('F R') /*Condición actual */
         )
    AND SUBSTR(ENDOSO,1,1)='1'
    and
/*Descarta las polizas contenidas en la lista de exclusion*/
    POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
;
Quit;  

*-----------------------------------------------------------------------*
* Obtiene todos los movimientos de las polizas que tuvieron estatus F/R *
*-----------------------------------------------------------------------*;
PROC SQL;  
	CREATE TABLE WORK.COBRANZA_T3 AS
	SELECT *
    FROM WORK.COBRANZA_T1 
	WHERE
		POLIZA 
			IN (SELECT POLIZA FROM WORK.COBRANZA_T2)
    ;
QUIT;
RUN;
*-------------------------------------------------------------------------*
* Identifica las polizas que tuvieron un pago de prima en sus movimientos *
*-------------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T4 AS
	SELECT t1.*, t2.CON_PMA_PAGADA
   	FROM WORK.COBRANZA_T3 AS t1
	LEFT JOIN
			(SELECT POLIZA, 
	           CASE WHEN MAX(PMA_PAGADA) >0 THEN 1
			   		WHEN MAX(PMA_PAGADA) <=0 THEN 0 
                    ELSE .
	           END 
				AS CON_PMA_PAGADA
			   FROM WORK.COBRANZA_T3
			  GROUP BY POLIZA) AS t2 
    ON t1.POLIZA = t2.POLIZA
    ;
QUIT;
RUN;
  

data  COBRANZA_T4_1;
   set  COBRANZA_T4;

   num_endoso=input(kstrip(endoso),best32.);
   num_inciso=input(kstrip(INCISO),best32.);

  /*Se omite los movimientos con  ESTATUS_INCISO="F R" ya que no afecta al estatus de la póliza
    FR= endoso por cambio de pago
  */
  if ( upcase(kstrip(ESTATUS_INCISO) ) ne  upcase("F R") ) and 
  /* Selecciona sólo las polizas con endoso 2 */
 SUBSTR(kstrip(ENDOSO),1,1)='2';    
run;

*-------------------------------------------*
* Obtiene el ultimo movimiendo de la poliza *
*-------------------------------------------*;
proc  sort data=COBRANZA_T4_1;
    BY POLIZA DESDE FEC_EMI num_inciso num_endoso;
quit;

DATA WORK.COBRANZA_T5;
 SET WORK.COBRANZA_T4_1;
  BY  POLIZA DESDE FEC_EMI num_inciso num_endoso;
  IF LAST.POLIZA THEN OUTPUT;
RUN;

*-----------------------------------------------*
* Genera el estatus de cancelacion de la poliza *
*-----------------------------------------------*;
DATA WORK.COBRANZA_T6;
 Set WORK.COBRANZA_T5;
  IF
	ESTATUS_INCISO = '0001-CANCELADO' AND
	REMESA = '9999999999' AND 
	USUARIO_EMI = 'CANCELA-AUT' AND
	ESTATUS = 'E' AND
	PMA_PAGADA = 0 AND 
	PMA_PENDIENTE = 0 AND
    CON_PMA_PAGADA = 1
    then ESTATUS_CANCELACIONES = 'CANCELADA CON PAGOS PARCIALES (AUTOMATICO)';
  ELSE IF
	ESTATUS_INCISO = '0001-CANCELADO' AND
	REMESA = '9999999999' AND 
	USUARIO_EMI ne 'CANCELA-AUT' AND
	ESTATUS = 'E' AND
	PMA_PAGADA = 0 AND 
	PMA_PENDIENTE = 0 AND
    CON_PMA_PAGADA = 1 
    THEN ESTATUS_CANCELACIONES = 'CANCELADA CON PAGOS PARCIALES (USUARIO)';
  ELSE IF 
	ESTATUS_INCISO = '0001-CANCELADO' AND
	REMESA = '9999999999' AND 
	USUARIO_EMI = 'CANCELA-AUT' AND
	ESTATUS = 'E' AND
	PMA_PAGADA = 0 AND 
	PMA_PENDIENTE = 0 AND
    CON_PMA_PAGADA = 0
    THEN ESTATUS_CANCELACIONES = 'CANCELADA SIN PAGOS (AUTOMATICO)';
  ELSE IF 
	ESTATUS_INCISO = '0001-CANCELADO' AND
	REMESA = '9999999999' AND 
	USUARIO_EMI ne 'CANCELA-AUT' AND
	ESTATUS = 'E' AND
	PMA_PAGADA = 0 AND 
	PMA_PENDIENTE = 0 AND
    CON_PMA_PAGADA = 0 
    THEN ESTATUS_CANCELACIONES = 'CANCELADA SIN PAGOS (USUARIO)';
  ELSE ESTATUS_CANCELACIONES = 'NO APLICA';
RUN;


*--------------------------------------------------------------------------*
* Obtiene el total de polizas por agente unico,no toma encuenta el estatus *
*--------------------------------------------------------------------------*;
*----------------- INICIO: <<IDENTIFICA REEXPEDICIONES>> -----------------*;
data input_COBRANZA_T1;
    set  COBRANZA_T1 (rename= (ENDOSO= endo));
  key_unique = _N_;
     if  kstrip(endo)='000000' and  missing(serie) = 0 ;
run;

data input_COBRANZA_T1_PRE;
   set COBRANZA_T1_PRE(rename= (ENDOSO= endo));
run;

%obtiene_reexpedida_reexpedicion(
   tabla_pol_indiv_endo_0=input_COBRANZA_T1, /*Tiene solo movimientos con endoso ='000000' */
   tabla_pol_indiv_all_endo=input_COBRANZA_T1_PRE,  /*Tiene  todos los movimientos */
   UMBRAL_REXP=0      /*parámetro de umbral, el valor de cero indica que traera todas las pólizas con reexpedición */
); 
*----------------- TERMINA: <<IDENTIFICA REEXPEDICIONES>> -----------------*;

PROC SQL;
CREATE TABLE WORK.COBRANZA_ALL_T7 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS CNT_ALL_POL
	FROM
		WORK.COBRANZA_T1 t1
	WHERE ENDOSO ='000000' and missing(CLAVE_UNICA_AGENTE ) =0 and missing(serie) = 0 
    and    t1.poliza in (select  distinct poliza from _OUT_REEXPEDIDA_REEXPEDICION 
                            where FLG_REEXPEDICION_V = 0     )
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
RUN;

*---------------------------------------------------------------*
* Obtiene el total de polizas canceladas sin pagos (automatico) *
*---------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T8 AS
	SELECT
		CLAVE_UNICA_AGENTE,
		count(distinct(t1.POLIZA)) as CNT_POL_CANCELA_AUT_SINPAGO
		FROM WORK.COBRANZA_T6 t1
	WHERE ESTATUS_CANCELACIONES = 'CANCELADA SIN PAGOS (AUTOMATICO)'
	GROUP BY CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*------------------------------------------------------------*
* Obtiene el total de polizas canceladas sin pagos (usuario) *
*------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T9 AS
	SELECT
		CLAVE_UNICA_AGENTE,
		count(distinct(t1.POLIZA)) as CNT_POL_CANCELA_USR_SINPAGO
		FROM WORK.COBRANZA_T6 t1
	WHERE ESTATUS_CANCELACIONES = 'CANCELADA SIN PAGOS (USUARIO)'
	GROUP BY CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*---------------------------------------------------------------*
* Obtiene el total de polizas canceladas con pagos (automatico) *
*---------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T10 AS
	SELECT
		CLAVE_UNICA_AGENTE,
		count(distinct(t1.POLIZA)) as CNT_POL_CANCELA_AUT_CONPAGO
		FROM WORK.COBRANZA_T6 t1
	WHERE ESTATUS_CANCELACIONES = 'CANCELADA CON PAGOS PARCIALES (AUTOMATICO)'
	GROUP BY CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*------------------------------------------------------------*
* Obtiene el total de polizas canceladas con pagos (usuario) *
*------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.COBRANZA_T11 AS
	SELECT
		CLAVE_UNICA_AGENTE,
		count(distinct(t1.POLIZA)) as CNT_POL_CANCELA_USR_CONPAGO
		FROM WORK.COBRANZA_T6 t1
	WHERE ESTATUS_CANCELACIONES = 'CANCELADA CON PAGOS PARCIALES (USUARIO)'
	GROUP BY CLAVE_UNICA_AGENTE;
QUIT;
RUN;
*----------------------------------------------------------------------*
* Integra los totales de las polizas canceladas (automatico - usuario) *
*----------------------------------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_4_RESULT) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
create table QCS_PREP_PREV_R1_4_RESULT_pre as
	SELECT DISTINCT
		t1.CLAVE_UNICA_AGENTE,
		t2.CNT_POL_CANCELA_AUT_SINPAGO,
		t3.CNT_POL_CANCELA_USR_SINPAGO,
		t4.CNT_POL_CANCELA_AUT_CONPAGO,
        t5.CNT_POL_CANCELA_USR_CONPAGO,
        SUM(t2.CNT_POL_CANCELA_AUT_SINPAGO,	
            t3.CNT_POL_CANCELA_USR_SINPAGO, 
            t4.CNT_POL_CANCELA_AUT_CONPAGO,
            t5.CNT_POL_CANCELA_USR_CONPAGO) AS CNT_POL_CANCELADAS,
        t6.CNT_ALL_POL as CNT_POL_VIG,   /* conte de todas las pólizas sin importar el estatus */
        /*DIVIDE(CALCULATED CNT_POL_CANCELADAS, SUM(CALCULATED CNT_POL_CANCELADAS, t6.CNT_POL_VIG)) as PCT_POL_CANCELADAS, */
       DIVIDE(CALCULATED CNT_POL_CANCELADAS,CNT_ALL_POL ) as PCT_POL_CANCELADAS, 
   	   DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID length=10,
       DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
	FROM
		WORK.COBRANZA_T6 t1
   /* Obtiene el total de polizas canceladas sin pagos (automatico) */
	LEFT JOIN WORK.COBRANZA_T8 t2 ON t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE
  /* Obtiene el total de polizas canceladas sin pagos (usuario) */
	LEFT JOIN WORK.COBRANZA_T9 t3 ON t1.CLAVE_UNICA_AGENTE=t3.CLAVE_UNICA_AGENTE
  /* Obtiene el total de polizas canceladas con pagos (automatico) */
	LEFT JOIN WORK.COBRANZA_T10 t4 ON t1.CLAVE_UNICA_AGENTE=t4.CLAVE_UNICA_AGENTE
  /* Obtiene el total de polizas canceladas con pagos (usuario) */
	LEFT JOIN WORK.COBRANZA_T11 t5 ON t1.CLAVE_UNICA_AGENTE=t5.CLAVE_UNICA_AGENTE
  /* Obtiene el total de polizas  por agente unico */
	LEFT JOIN WORK.COBRANZA_ALL_T7 t6 ON t1.CLAVE_UNICA_AGENTE=t6.CLAVE_UNICA_AGENTE
    WHERE t1.ESTATUS_CANCELACIONES ne 'NO APLICA'; 
QUIT;
RUN;

proc append data=QCS_PREP_PREV_R1_4_RESULT_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_4_RESULT;
run;

*-----------------------------------------*
* Genera las Prep Tables Motora y Detalle *
*-----------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_4_ENGINE) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
create table QCS_PREP_PREV_R1_4_ENGINE_pre as 
    SELECT POLIZA,
           ENDOSO,
           INCISOS,
           DESDE,
           HASTA,
           FEC_EMI,
           ESTATUS_INCISO,
           REMESA,
		   USUARIO_EMI,
		   ESTATUS,
		   PMA_PAGADA,
		   PMA_PENDIENTE,
           RECIBOS-RECIBOS_PAGADOS as RECIBOS_PENDIENTES format=COMMA12.,
           ESTATUS_CANCELACIONES,        
           AGENTE,
           NOM_AGTE,
           CLAVE_UNICA_AGENTE,
       	   DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID length=10,
           DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
      FROM WORK.COBRANZA_T6;
QUIT;
RUN;

proc append data=QCS_PREP_PREV_R1_4_ENGINE_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_4_ENGINE;
run;

*-----------------------------------------*
* Obtiene las polizas con movimientos F/R *
*-----------------------------------------*;
PROC SQL;
  CREATE TABLE Work.COBRANZA_T12 AS
  SELECT t2.POLIZA, t2.ENDOSO, t2.INCISO, t2.INCISOS, t2.SERIE, t2.DESDE, t2.HASTA, t2.FEC_EMI, t2.ESTATUS,
         t2.PMA_PAGADA, t2.REMESA, t2.USUARIO_EMI, t2.RECIBOS, t2.RECIBOS_PAGADOS, t2.PRIMA_TOTAL, t2.PMA_PENDIENTE, t2.SUBSEC,
         t2.PRIMER_REC, t2.ESTATUS_INCISO, t2.AGENTE, t2.ASEG, t2.DESCRIP, t2.FORMA_PAGO, t2.SINIESTROS, t2.USO, t2.NOM_OFIC,
         t2.CVE_AGTE, t2.NOM_AGTE, t2.CLAVE_UNICA_AGENTE, t2.GERENTE, t2.row_unique

    FROM COBRANZA_T1 AS t2 /*Tabla que contiene todos los movimientos del periodo seleccionado */ 
 
  /*Incluye la información de agente unico*/
  /*LEFT JOIN CONTROL_DE_AGENTES AS t3 ON t2.AGENTE=t3.CVE_AGTE */


  /*Seleccion sólo los endosos F/R*/
  WHERE   (  UPCASE(kstrip(t2.ESTATUS))='F/R' OR  
             upcase(kstrip(ESTATUS_INCISO)) = upcase('F R') /*Condición actual */
          )
		/*Descarta las polizas contenidas en la lista de exclusion*/
		and t2.POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
		/*Descarta los agentes contenidos en la lista de exclusion*/
		and t2.AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE)
;
QUIT;
RUN;
*---------------------------------------------------------------------------------*
* Obtiene todos los movimientos que tuvieron estatus F/R de las polizas alertadas *
*---------------------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T13 AS
	SELECT *
    FROM WORK.COBRANZA_T12 
	WHERE
		POLIZA 
			IN (SELECT DISTINCT POLIZA FROM &LIBN_SAS..QCS_PREP_PREV_R1_4_ENGINE WHERE ESTATUS_CANCELACIONES NE 'NO APLICA'
               );
QUIT;
RUN;
*------------------------------------------------------------------*
* Obtiene todos los movimientos de endoso de las polizas alertadas *
*------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T14 AS
	SELECT *
    FROM WORK.COBRANZA_T3 
	WHERE
		POLIZA 
			IN (SELECT DISTINCT POLIZA FROM &LIBN_SAS..QCS_PREP_PREV_R1_4_ENGINE
      WHERE ESTATUS_CANCELACIONES NE 'NO APLICA' );
QUIT;
RUN;


*------------------------------------------*
* Integra todos los endosos de las polizas *
*------------------------------------------*;
DATA WORK.COBRANZA_T15;
 SET WORK.COBRANZA_T14 
     WORK.COBRANZA_T13;
 RECIBOS_PENDIENTES = RECIBOS-RECIBOS_PAGADOS;
 FORMAT RECIBOS_PENDIENTES COMMA12.;
RUN;

/*Elimina pólizas con endosos que ya se encuentran en la base COBRANZA_T14 */
proc sort data=COBRANZA_T15 nodupkey;
 by row_unique;
quit;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_4_DETAIL) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
create table QCS_PREP_PREV_R1_4_DETAIL_pre as
  SELECT t1.POLIZA,
         t1.ENDOSO,
         t1.INCISO,
         t1.ESTATUS_INCISO,
         t1.ESTATUS,
         t1.SERIE,
         t1.FEC_EMI,
         t1.DESDE,
         t1.HASTA,
         t1.ASEG,
         t1.AGENTE,
         t1.NOM_AGTE,
         t1.PRIMA_TOTAL,
         t1.PMA_PAGADA,
         t1.PMA_PENDIENTE,
         t1.SUBSEC,
         t1.PRIMER_REC,
         t1.REMESA,
         t1.USUARIO_EMI,
         t1.DESCRIP,
         t1.FORMA_PAGO,
         t1.SINIESTROS,
         t1.RECIBOS,
         t1.RECIBOS_PAGADOS,
         t1.RECIBOS_PENDIENTES,
         t1.USO,
         t1.NOM_OFIC,
         t1.GERENTE,
         t1.CLAVE_UNICA_AGENTE,
         t2.ESTATUS_CANCELACIONES,
	   	 DATETIME() as BATCHDATE,
	     "&SYSUSERID." AS SYSUSERID length=10,
	     DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
    FROM WORK.COBRANZA_T15 AS t1
  LEFT JOIN WORK.COBRANZA_T6 AS t2 
	ON t1.POLIZA=t2.POLIZA AND
       t1.ENDOSO=t2.ENDOSO AND
       t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE;
QUIT;
RUN;

proc append data=QCS_PREP_PREV_R1_4_DETAIL_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_4_DETAIL force;
run;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;