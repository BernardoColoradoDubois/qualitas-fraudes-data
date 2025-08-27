*****************************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                                          *
* FECHA: Septiembre 3, 2021                                                                                       *
* TITULO: Creación de Prep-Table Familia 2 para la regla 2.1                                                    *
* OBJETIVO: Aplicación de la Nota de Crédito para pago de primas de pólizas-endosos de asegurados distintos     *
* ENTRADAS: COBRANZA, CONTROL_DE_AGENTES, CLIENTE_UNICO_H                                                       *
* SALIDAS: QCS_PREP_PREV_R2_1_ENGINE, QCS_PREP_PREV_R2_1_DETAIL, QCS_PREP_PREV_R2_1_RESULT                      *
*****************************************************************************************************************;
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
readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;

Libname &LIBN_SAS. oracle path=&Res_ora_path. schema="&Res_ora_schema." user="&Res_ora_user." password=&Res_ora_password.
readbuff=32767 insertbuff=32767
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
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R2_1
	WHERE NAME_OF_THE_RULE='PREVENCION 2.1' AND PARAMETER_NAME='LISTA_POLIZAS';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Agentes  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AGENTE AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R2_1
	WHERE NAME_OF_THE_RULE='PREVENCION 2.1' AND PARAMETER_NAME='LISTA_AGENTE';
QUIT;
RUN;
*--------------------------------------------------------------------*
* Identifica las polizas individuales de cobranza                    *
* Integra la informacion del agente unico  (Agente, Gerente)         *
* Integra la informacion del cliente unico (Asegurado Golden Record) *
*--------------------------------------------------------------------*;

proc sql;
create table COBRANZA_M as
select * from &LIBN_ORA..COBRANZA  
where
	POL not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS) AND
	AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE)
;
quit;
  
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


PROC SQL;
CREATE TABLE CLIENTE_UNICO_H_GR AS 
	SELECT DISTINCT
       ASEGURADO_GR,
       ASEG as ASEG_GR,
	   LENGTH(TRIM(LEFT(ASEG))) AS LONG,
       MAX(CALCULATED LONG) AS MAX,
       COUNT(DISTINCT ASEG) AS N,
       FEC_EMI,
	   LAST_UPDATE_TIME
    FROM &LIBN_SAS..CLIENTE_UNICO_H
    GROUP BY ASEGURADO_GR
    HAVING LONG = MAX
	ORDER BY N DESC, ASEGURADO_GR, MAX DESC, LONG DESC, FEC_EMI DESC
;
QUIT;

DATA CLIENTE_UNICO_H_GR_F;
SET CLIENTE_UNICO_H_GR;
BY descending N ASEGURADO_GR descending MAX descending LONG descending FEC_EMI;
if first.ASEGURADO_GR;
RUN;

PROC SQL;
CREATE TABLE CLIENTE_UNICO_H_GR_FINAL AS
SELECT DISTINCT T1.ASEGURADO, 
                T1.ASEGURADO_GR,
			    T2.ASEG_GR
  FROM (SELECT DISTINCT ASEGURADO, ASEGURADO_GR FROM &LIBN_SAS..CLIENTE_UNICO_H) T1
  LEFT JOIN (SELECT DISTINCT ASEGURADO_GR,ASEG_GR FROM CLIENTE_UNICO_H_GR_F) T2
  ON(T1.ASEGURADO_GR = T2.ASEGURADO_GR );
QUIT;

proc sort data=CLIENTE_UNICO_H_GR_FINAL;
  by ASEGURADO_GR  ASEGURADO;
quit;

/*Obtiene datos del agente*/
PROC SQL;
CREATE TABLE CONTROL_DE_AGENTES AS
SELECT DISTINCT
                   CVE_AGENTE AS CVE_AGTE, 
                   AGENTE_NOM AS NOM_AGTE, 
                   CLAVE_UNICA_AGENTE,
                   NOMBRE_GERENTE_INC27 AS GERENTE
               FROM &LIBN_ORA..CONTROL_DE_AGENTES;
QUIT;

proc sort data=CONTROL_DE_AGENTES nodupkey;
  by CLAVE_UNICA_AGENTE CVE_AGTE ;
quit;

PROC SQL;
    CREATE TABLE WORK.COBRANZA_M2 AS
        SELECT 
            t2.POL AS POLIZA, 
            t2.ENDO, 
            t2.INCISO, 
            t2.INCISOS,
			t2.SERIE, 
            t2.DESDE, 
            t2.HASTA, 
            t2.FEC_EMI, 
            t2.REMESA, 
            t2.USUARIO_EMI,
			t2.ESTATUS_INCISO,
            t2.PMA_PAGADA, 
            t2.PRIMA_TOTAL, 
            t2.DESCRIP, 
            t2.FORMA_PAGO, 
            t2.PMA_PENDIENTE, 
            t2.SINIESTROS, 
            t2.PRIMER_REC, 
            t2.USO, 
            t2.NOM_OFIC, 
            t2.AGENTE, 
            t2.ASEGURADO,
            t2.ASEG,
            t2.SUBRAMO, 
            t2.BATCHDATE 
        FROM  COBRANZA_M t2  
    /*Obtiene los movimientos de las polizas individuales*/
    INNER JOIN COBRANZA_PII t1
           ON t2.POL=t1.POL;
quit;

/*Se valida registros duplicados partiendo de la llave  
  subramo, pol, endo e inciso que hace un registro unico en la fuente de cobranza
*/
proc sort data=COBRANZA_M2;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
run;

data COBRANZA_M2;
set COBRANZA_M2;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
 key_uniq_cobra=_N_;

if last.SUBRAMO;
drop SUBRAMO BATCHDATE;
run;

proc sql;
create table COBRANZA_T1 as
 select  c.*,
            case when missing(t4.ASEGURADO_GR) = 0 then  t4.ASEGURADO_GR
              else c.ASEGURADO
            end  as ASEGURADO_GR length=10,
            case when missing( t4.ASEG_GR ) = 0 then t4.ASEG_GR 
             else c.ASEG 
            end as ASEG_GR  length=100,
            t3.CVE_AGTE, 
            t3.NOM_AGTE, 
            t3.CLAVE_UNICA_AGENTE, 
            t3.GERENTE

 from  COBRANZA_M2 c
    /*Incluye la información de agente unico*/
    LEFT JOIN CONTROL_DE_AGENTES t3 
           ON c.AGENTE=t3.CVE_AGTE

    /*Incluye la información de cliente unico*/
    LEFT JOIN CLIENTE_UNICO_H_GR_FINAL t4 
           ON c.ASEGURADO=t4.ASEGURADO;
QUIT;

*-------------------------------------------------------------------------*
* Identifica las notas de credito de cancelaciones (polizas con endoso 2) *
* Identifica las polizas con movimientos de endosos 0 y 1                 *
*-------------------------------------------------------------------------*;
DATA WORK.COBRANZA_T2 WORK.COBRANZA_T3;
 SET WORK.COBRANZA_T1;
 PERIOD_BATCHDATE = DHMS(&fec_fin.,0,0,0);

/*
considerar endosos con inicio 2, los cuales tengan una remesa diferente a “999999999” O VACIA”
pero debido a que la base de cobranza cuenta con “0” en lugar de vacío este también debe ser considerado como parte del cálculo.
*/
 IF SUBSTR(kstrip(ENDO),1,1)='2' AND missing(kstrip(REMESA)) = 0 AND  kstrip(REMESA) NOT IN ("9999999999","0")  THEN OUTPUT WORK.COBRANZA_T2;

/*Obtiene pólizas con movimientos 0 y 1 */
 IF SUBSTR(kstrip(ENDO),1,1) in ('0','1') THEN OUTPUT WORK.COBRANZA_T3;
RUN;
*-----------------------------------------------------------------------------------*
* Filtra unicamente las notas de credito de cancelacion en las polizas endoso 0 y 1 *
*-----------------------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T4 AS
		SELECT * FROM WORK.COBRANZA_T3 t1
	WHERE kstrip(REMESA) IN (SELECT DISTINCT kstrip(REMESA) FROM WORK.COBRANZA_T2);
QUIT;
RUN;

proc sort data=COBRANZA_T2   out=COBRANZA_T2;
 by REMESA;
quit;
proc sort data=COBRANZA_T4  out=COBRANZA_T4;
 by REMESA;
quit;

*-------------------------------------------------------------------------------*
* Identifica las notas de credito de cancelacion aplicadas en los endosos 0 y 1 *
*-------------------------------------------------------------------------------*;
PROC SQL;
    CREATE TABLE WORK.COBRANZA_T5 AS
	SELECT t1.POLIZA AS POLIZA_ORIGEN,
           t1.ENDO AS ENDO_ORIGEN,
           t1.ASEG_GR AS ASEG_GR_ORIGEN,
           t1.ASEG AS  ASEG_ORIGEN,
           t1.INCISO AS INCISO_ORIGEN,
		   t1.DESDE AS DESDE_ORIGEN,
		   t1.HASTA AS HASTA_ORIGEN,
		   t1.FEC_EMI AS FEC_EMI_ORIGEN,
		   t1.INCISOS AS INCISOS_ORIGEN,
           t1.DESCRIP AS DESCRIP_ORIGEN,
		   t1.SERIE AS SERIE_ORIGEN,
		   t1.AGENTE AS AGENTE_ORIGEN,
		   t1.NOM_AGTE AS NOM_AGTE_ORIGEN,
		   t1.CLAVE_UNICA_AGENTE AS CLAVE_UNICA_AGENTE_ORIGEN,
           t1.REMESA AS REMESA_ORIGEN,
		   t1.USUARIO_EMI AS USUARIO_EMI_ORIGEN,
		   t1.NOM_OFIC AS NOM_OFIC_ORIGEN,
		   t1.ESTATUS_INCISO AS ESTATUS_INCISO_ORIGEN,
           t1.ASEGURADO_GR AS ASEGURADO_GR_ORIGEN,
           t1.ASEGURADO AS ASEGURADO_ORIGEN,  
		   t1.PMA_PAGADA AS PMA_PAGADA_ORIGEN,
           t1.PRIMA_TOTAL AS PRIMA_TOTAL_ORIGEN,
		   t1.PMA_PENDIENTE AS PMA_PENDIENTE_ORIGEN,
		   t2.POLIZA,
		   t2.ENDO,
		   t2.ASEG_GR,
           t2.ASEG,
           t2.INCISO,
		   t2.DESDE,
		   t2.HASTA,
		   t2.FEC_EMI,
		   t2.INCISOS,
           t2.DESCRIP,
		   t2.SERIE,
		   t2.AGENTE,
		   t2.NOM_AGTE,
           t2.GERENTE,
		   t2.CLAVE_UNICA_AGENTE,
		   t2.REMESA,
		   t2.USUARIO_EMI,
           t2.FORMA_PAGO,
		   t2.SINIESTROS,
		   t2.USO,
		   t2.NOM_OFIC,
		   t2.ESTATUS_INCISO,
		   t2.ASEGURADO_GR,
           t2.ASEGURADO,
		   t2.PMA_PAGADA,
           t2.PRIMA_TOTAL,
		   t2.PMA_PENDIENTE,
           t2.PRIMER_REC

    FROM WORK.COBRANZA_T2 t1

    LEFT JOIN WORK.COBRANZA_T4 t2 ON t1.REMESA=t2.REMESA

    WHERE missing(kstrip(t2.POLIZA)) = 0  AND /*Descarta remesas no encontradas*/
          t1.POLIZA NE t2.POLIZA AND /*Diferente Poliza*/
          t1.ASEGURADO_GR NE t2.ASEGURADO_GR /*Diferente Asegurado*/
;
QUIT;
RUN;

*--------------------------------------------------------*
* Consolida las polizas de endosos 0, 1 y 2 del analisis *
*--------------------------------------------------------*;
DATA WORK.COBRANZA_T6;
 SET WORK.COBRANZA_T2 WORK.COBRANZA_T4;
RUN;
*-----------------------------------------------------*
* Genera las prep tables Motora, Detalle y Resultados *
*-----------------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R2_1_ENGINE) by oracle;
disconnect from oracle;
QUIT;
    
PROC SQL;
	CREATE TABLE QCS_PREP_PREV_R2_1_ENGINE AS
		SELECT
			POLIZA,
			ENDO,
            ASEG,
            ASEGURADO,
 			ASEG_GR,
 			ASEGURADO_GR,
            INCISO,
			DESDE,
			HASTA,
			FEC_EMI,
			INCISOS,
            DESCRIP,
			SERIE,
			AGENTE,
			NOM_AGTE,
			CLAVE_UNICA_AGENTE,
			REMESA,
			USUARIO_EMI LENGTH=200 format=$200.,
			NOM_OFIC,
			ESTATUS_INCISO,
			PMA_PAGADA,
            PRIMA_TOTAL,
			PMA_PENDIENTE,
	   	   DATETIME() as BATCHDATE,
	       "&SYSUSERID." AS SYSUSERID LENGTH=40,
	       DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
	FROM WORK.COBRANZA_T6;
QUIT;

PROC APPEND DATA=QCS_PREP_PREV_R2_1_ENGINE BASE=&LIBN_SAS..QCS_PREP_PREV_R2_1_ENGINE force;
RUN;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R2_1_DETAIL) by oracle;
disconnect from oracle;
QUIT;

data pre_QCS_PREP_PREV_R2_1_DETAIL;
   set COBRANZA_T5
(keep=
POLIZA_ORIGEN
ENDO_ORIGEN
ASEG_GR_ORIGEN
ASEGURADO_GR_ORIGEN
ASEG_GR
ASEGURADO_GR
ASEG
ASEGURADO
ASEG_ORIGEN
ASEGURADO_ORIGEN
INCISO_ORIGEN
DESDE_ORIGEN
HASTA_ORIGEN
FEC_EMI_ORIGEN
INCISOS_ORIGEN
DESCRIP_ORIGEN
SERIE_ORIGEN
AGENTE_ORIGEN
NOM_AGTE_ORIGEN
CLAVE_UNICA_AGENTE_ORIGEN
REMESA_ORIGEN
USUARIO_EMI_ORIGEN
NOM_OFIC_ORIGEN
ESTATUS_INCISO_ORIGEN
PMA_PAGADA_ORIGEN
PRIMA_TOTAL_ORIGEN
PMA_PENDIENTE_ORIGEN
POLIZA
ENDO
ASEG
ASEGURADO
ASEG_GR
ASEGURADO_GR
INCISO
DESDE
HASTA
FEC_EMI
INCISOS
DESCRIP
SERIE
AGENTE
NOM_AGTE
GERENTE
CLAVE_UNICA_AGENTE
REMESA
USUARIO_EMI
FORMA_PAGO
SINIESTROS
USO
NOM_OFIC
ESTATUS_INCISO
PMA_PAGADA
PRIMA_TOTAL
PMA_PENDIENTE
PRIMER_REC
);   

 length SYSUSERID $40.;

 BATCHDATE = DATETIME();
 SYSUSERID = "&SYSUSERID.";
 PERIOD_BATCHDATE = DHMS(&fec_fin.,0,0,0); 
run;

PROC APPEND DATA=pre_QCS_PREP_PREV_R2_1_DETAIL BASE=&LIBN_SAS..QCS_PREP_PREV_R2_1_DETAIL force;
RUN;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R2_1_RESULT) by oracle;
disconnect from oracle;
QUIT;
PROC SQL;
	create table PRE_QCS_PREP_PREV_R2_1_RESULT as
		SELECT
			t1.NOM_OFIC AS OFICINA length=100 format=$100. informat=$100.,
			COUNT(DISTINCT(t1.REMESA)) AS NOTAS_DE_CREDITO,
			COUNT(DISTINCT(t1.USUARIO_EMI)) AS USUARIOS,
			COUNT(DISTINCT(t1.AGENTE)) AS AGENTES,
			SUM(t1.PMA_PAGADA) AS TOTAL_PRIMAS,
			COUNT(DISTINCT(t1.POLIZA_ORIGEN)) AS POLIZA_ORIGEN_NC,
            COUNT(DISTINCT(t1.POLIZA)) AS POLIZA_DE_APLICACION,
	   	   DATETIME() as BATCHDATE,
	       "&SYSUSERID." AS SYSUSERID length=40,
	        DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.COBRANZA_T5 t1
             where missing(t1.NOM_OFIC) = 0
        GROUP BY
			t1.NOM_OFIC   /* Oficina de pólizas con movimientos 0 y 1 */
;
QUIT;
RUN;

proc  append  base= &LIBN_SAS..QCS_PREP_PREV_R2_1_RESULT   data=PRE_QCS_PREP_PREV_R2_1_RESULT force; 
quit;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;