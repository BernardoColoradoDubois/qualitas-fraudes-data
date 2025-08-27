***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 12, 2021                                                                         *
* TITULO: Creación de Score para la Regla 6.3                                                     *
* OBJETIVO: Determinar Score para la familia 6 en la regla 6.3                                    *
* ENTRADAS: QCS_PREP_PREV_R6_3_RESULT                                                             *
* SALIDAS: QCS_PREP_PREV_R6_3_SCORE                                                               *
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

*----------------------------------------*
* Obtiene la frecuencia por el ajustador *
*----------------------------------------*;
PROC SQL;  
	CREATE TABLE WORK.FREC_AJUS AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.AJUSTADOR,
			COUNT(t1.AJUSTADOR) AS FREC_AJUS
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_3_RESULT t1
	GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;

proc sort data=WORK.FREC_AJUS nodupkey;
   by CVE_NOMBRE_AJUSTADOR_GR;
quit;
*------------------------------------------------*
* Obtiene la suma del importe de pagos ajustador *
*------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.SUM_IMP_PAGO AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			SUM(t1.IMPORTE_PAGO_MAYOR_7M) AS SUM_IMP_PAGO
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_3_RESULT t1
	GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;
*-----------------------------------------*
* Consolida las metricas por el ajustador *
*-----------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.UNE_FREC_AJUS AS
		SELECT
            t1.CVE_NOMBRE_AJUSTADOR_GR,
			t1.AJUSTADOR,
			t1.FREC_AJUS,
			t2.SUM_IMP_PAGO
		FROM
			WORK.FREC_AJUS t1
				INNER JOIN WORK.SUM_IMP_PAGO t2 ON (t1.CVE_NOMBRE_AJUSTADOR_GR = t2.CVE_NOMBRE_AJUSTADOR_GR)
	;
QUIT;
RUN;
*------------------------------------*
* Asigna los rangos a los parametros *
*------------------------------------*;
PROC RANK DATA=WORK.UNE_FREC_AJUS OUT=WORK.RANKS;
   VAR FREC_AJUS SUM_IMP_PAGO;
   RANKS RANK1_FREC_AJUS RANK2_SUM_IMP_PAGO;
RUN;
*-------------------------------*
* Obtiene la suma de los rangos *
*-------------------------------*;
PROC SQL;
	CREATE TABLE WORK.RANK_SUM AS
		SELECT
			t1.*,
			SUM(t1.RANK1_FREC_AJUS, RANK2_SUM_IMP_PAGO ) AS RANK_SUM
		FROM
			WORK.RANKS t1
	;
QUIT;
RUN;
*--------------------------------------------*
* Obtiene el maximo de la suma de los rangos *
*--------------------------------------------*;
PROC SQL NOPRINT;
    SELECT MAX(RANK_SUM)
     INTO :MAX_RANK_SUM
        FROM WORK.RANK_SUM;
QUIT;
RUN;
%put &=MAX_RANK_SUM;
*---------------------------------*
* Calcular el score por ajustador *
*---------------------------------*;

PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R6_3_SCORE AS
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR, 
            AJUSTADOR,
			FREC_AJUS,
			SUM_IMP_PAGO,
			RANK1_FREC_AJUS,
			RANK2_SUM_IMP_PAGO,
			RANK_SUM,
            DIVIDE(RANK_SUM, &MAX_RANK_SUM.) AS SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID
		FROM
			WORK.RANK_SUM;
QUIT;
RUN;
PROC SORT DATA=WORK.QCS_PREP_PREV_R6_3_SCORE;
BY DESCENDING SCORE;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 6.3 *
*---------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_3_SCORE 
         	where trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
  ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_3_SCORE
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR,
            AJUSTADOR,
			FREC_AJUS,
			SUM_IMP_PAGO,
			RANK1_FREC_AJUS,
			RANK2_SUM_IMP_PAGO,
			RANK_SUM,
            SCORE,
            BATCHDATE,
            SYSUSERID,
       DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R6_3_SCORE;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_3_SCORE 
     where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;