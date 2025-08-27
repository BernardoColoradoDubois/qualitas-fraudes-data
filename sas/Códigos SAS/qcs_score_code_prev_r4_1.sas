***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 13, 2021                                                                         *
* TITULO: Creación de Score para la Regla 4.1                                                     *
* OBJETIVO: Determinar Score para la familia 4 en la regla 4.1                                    *
* ENTRADAS: QCS_PREP_PREV_R4_1_ENGINE                                                             *
* SALIDAS: QCS_PREP_PREV_R4_1_SCORE                                                               *
***************************************************************************************************;
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


Libname &LIBN_ORA. oracle path=&Ins_ora_path. schema="&Ins_ora_schema." user="&Ins_ora_user." password=&Ins_ora_password.;

Libname &LIBN_SAS. oracle path=&Res_ora_path. schema="&Res_ora_schema." user="&Res_ora_user." password=&Res_ora_password.
readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;


*---------------------------------------------------*
* Obtiene datos del periodo                         *
*---------------------------------------------------*;

/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

*---------------------------------------------------*
* Creación inicial de la tabla oracle para el Score *
*---------------------------------------------------*;
%macro crea_estructura_tablas_oracle;
%if not(%sysfunc(exist(&LIBN_SAS..QCS_PREP_PREV_R4_1_SCORE))) %then %do;
Proc sql;
create table &LIBN_SAS..QCS_PREP_PREV_R4_1_SCORE  (
   TIPO_PERSONA char(10) format=$10. informat=$10. label='TIPO_PERSONA',
   BENEFICIARIO char(300) format=$300. informat=$300. label='BENEFICIARIO',
   SUM_PAG_N num label='SUM_PAG_N',
   SUM_PAG_SUM num label='SUM_PAG_SUM',
   RANK1_SUM_PAG_N num label='RANK1_SUM_PAG_N',
   RANK2_SUM_PAG_SUM num label='RANK2_SUM_PAG_SUM',
   RANK_SUM num label='RANK_SUM',
   SCORE num label='SCORE',
   BATCHDATE num format=DATETIME20. informat=DATETIME20. label='BATCHDATE',
   SYSUSERID char(40) format=$40. informat=$40. label='SYSUSERID',
   PERIOD_BATCHDATE num format=DATETIME20. informat=DATETIME20. label='PERIOD_BATCHDATE'
  );
Quit;
%end;
%mend;
%crea_estructura_tablas_oracle;
*-------------------------------------------------*
* Obtiene la frecuencia de pagos por beneficiario *
*-------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.BENEF_ALERTADO AS
		SELECT 
			t1.STD_BENEFICIARIO AS BENEFICIARIO,
            t1.TIPO_PERSONA,
            t1.SUM_IMPORTE_PAGO,
			t1.PAGOS_X_BENEFICIARIO AS SUM_PAG_N
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R4_1_ENGINE t1
	   where  flg_alert_pago=1 and  PERIOD_BATCHDATE=&fec_fin.;	
	;
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la suma de pagos por beneficiario *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.SUM_PAG_SUM AS
		SELECT
			t1.BENEFICIARIO,
			SUM(t1.SUM_IMPORTE_PAGO) AS SUM_PAG_SUM
		FROM
	      	WORK.BENEF_ALERTADO t1
	GROUP BY
			t1.BENEFICIARIO;
QUIT;
RUN;

proc sort  data=WORK.BENEF_ALERTADO out=BENEF_ALERTADO_UNI  nodupkey;
	 by  BENEFICIARIO;
quit;

*-----------------------------------------*
* Consolida las metricas del beneficiario *
*-----------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.CONSOLIDA_METRICAS AS
		SELECT
			t2.TIPO_PERSONA,
			t1.BENEFICIARIO,
			t2.SUM_PAG_N,
			t1.SUM_PAG_SUM
		FROM
			WORK.SUM_PAG_SUM  t1
				INNER JOIN WORK.BENEF_ALERTADO_UNI t2 ON (t1.BENEFICIARIO = t2.BENEFICIARIO)
	;
QUIT;
RUN;

data  CONSOLIDA_METRICAS;
	  set  CONSOLIDA_METRICAS;
	 if  missing(BENEFICIARIO) ne 1;   
run;	  
	  

*------------------------------------*
* Asigna los rangos a los parametros *
*------------------------------------*;
PROC RANK DATA=WORK.CONSOLIDA_METRICAS OUT=WORK.RANKS TIES=MEAN DESCENDING;
   VAR SUM_PAG_N SUM_PAG_SUM;
   RANKS RANK1_SUM_PAG_N RANK2_SUM_PAG_SUM;
RUN;
*-------------------------------*
* Obtiene la suma de los rangos *
*-------------------------------*;
PROC SQL;
	CREATE TABLE WORK.RANK_SUM AS
		SELECT
			t1.*,
			SUM(t1.RANK1_SUM_PAG_N, t1.RANK2_SUM_PAG_SUM) AS RANK_SUM
		FROM
			WORK.RANKS t1
	;
QUIT;
RUN;
*--------------------------------------------*
* Obtiene el minimo de la suma de los rangos *
*--------------------------------------------*;
PROC SQL NOPRINT;
    SELECT MIN(RANK_SUM)	
     INTO :MIN_RANK_SUM
        FROM WORK.RANK_SUM;
QUIT;
RUN;
*------------------------------------*
* Calcular el score por beneficiario *
*------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R4_1_SCORE AS
		SELECT 
			TIPO_PERSONA,
            BENEFICIARIO,
			SUM_PAG_N,
			SUM_PAG_SUM,
			RANK1_SUM_PAG_N,
			RANK2_SUM_PAG_SUM,
			RANK_SUM,
            DIVIDE(&MIN_RANK_SUM.,RANK_SUM) AS SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID,
            DHMS(&fec_fin., 0, 0, 0) as PERIOD_BATCHDATE format=DATETIME20.
		FROM
			WORK.RANK_SUM
    ORDER BY SUM_PAG_N DESC, SCORE  ;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 4.1 *
*---------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from  QCS_PREP_PREV_R4_1_SCORE
         where  trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
        ) by oracle;
disconnect from oracle;
QUIT;


PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R4_1_SCORE
		SELECT 
            TIPO_PERSONA,
            BENEFICIARIO,
			SUM_PAG_N,
			SUM_PAG_SUM,
			RANK1_SUM_PAG_N,
			RANK2_SUM_PAG_SUM,
			RANK_SUM,
            SCORE,
            BATCHDATE,
            SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R4_1_SCORE;
QUIT;
RUN;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from  QCS_PREP_PREV_R4_1_SCORE
         where  trunc(PERIOD_BATCHDATE) <= TRUNC(TO_DATE(&fec_borra_ora, 'YYYY-MM-DD HH24:MI:SS'))
        ) by oracle;
disconnect from oracle;
QUIT;


Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;