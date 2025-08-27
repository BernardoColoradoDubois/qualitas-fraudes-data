***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 11, 2021                                                                         *
* TITULO: Creación de Score para la Regla 6.2                                                     *
* OBJETIVO: Determinar Score para la familia 6 en la regla 6.2                                    *
* ENTRADAS: QCS_PREP_PREV_R6_2_ENGINE, QCS_PREP_PREV_R6_2_RESULT                                  *
* SALIDAS: QCS_PREP_PREV_R6_2_SCORE                                                               *
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

*-----------------------------------------------------------*
* Obtiene el Número de pagos a beneficiarios por ajustador. *
*-----------------------------------------------------------*;

proc sort data=&LIBN_SAS..QCS_PREP_PREV_R6_2_RESULT out=pre_freq_ajus nodupkey;
   by CVE_NOMBRE_AJUSTADOR_GR BENEFICIARIO_GR;
quit;

PROC SQL;    
	CREATE TABLE WORK.FREC_AJUS AS  
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
             t1.AJUSTADOR, 
			sum(t1.NUM_PAGOS) AS FREC_AJUS
		FROM
			pre_freq_ajus t1
	GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;

proc sort data=FREC_AJUS nodupkey;
  by  CVE_NOMBRE_AJUSTADOR_GR;
quit;

*---------------------------------------------------------------*
* Obtiene el numero de beneficiarios atendidos por el ajustador *
*---------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.NUM_BENEF AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			COUNT(DISTINCT t1.BENEFICIARIO_GR) AS NUM_BENEF
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_2_RESULT t1
	GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;
*------------------------------------------------------------------------*
* Obtiene el numero minimo y maximo de pagos realizados por el ajustador *
*------------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.MIN_MAX AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			MIN(t1.NUM_PAGOS) AS COUNT_MIN,
            MAX(t1.NUM_PAGOS) AS COUNT_MAX
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_2_RESULT t1
	GROUP BY t1.CVE_NOMBRE_AJUSTADOR_GR;
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
			t2.NUM_BENEF,
			t3.COUNT_MIN,
			t3.COUNT_MAX
		FROM
			WORK.FREC_AJUS t1
				INNER JOIN WORK.NUM_BENEF t2 ON (t1.CVE_NOMBRE_AJUSTADOR_GR = t2.CVE_NOMBRE_AJUSTADOR_GR)
				INNER JOIN WORK.MIN_MAX t3 ON (t1.CVE_NOMBRE_AJUSTADOR_GR = t3.CVE_NOMBRE_AJUSTADOR_GR)
	;
QUIT;
RUN;
*------------------------------------*
* Asigna los rangos a los parametros *
*------------------------------------*;
PROC RANK DATA=WORK.UNE_FREC_AJUS OUT=WORK.RANKS TIES=MEAN;
   VAR FREC_AJUS NUM_BENEF COUNT_MIN COUNT_MAX;
   RANKS RANK1_FREC_AJUS RANK2_NUM_BENEF RANK3_COUNT_MIN RANK4_COUNT_MAX;
RUN;
*-------------------------------*
* Obtiene la suma de los rangos *
*-------------------------------*;
PROC SQL;
	CREATE TABLE WORK.RANK_SUM AS
		SELECT
			t1.*,
			SUM(t1.RANK1_FREC_AJUS, t1.RANK2_NUM_BENEF, t1.RANK3_COUNT_MIN, t1.RANK4_COUNT_MAX ) AS RANK_SUM
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
	CREATE TABLE WORK.QCS_PREP_PREV_R6_2_SCORE AS
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR,
            AJUSTADOR,
			FREC_AJUS,
			NUM_BENEF,
			COUNT_MIN,
			COUNT_MAX,
			RANK1_FREC_AJUS,
			RANK2_NUM_BENEF,
			RANK3_COUNT_MIN,
			RANK4_COUNT_MAX,
			RANK_SUM,
            DIVIDE(RANK_SUM, &MAX_RANK_SUM.) AS SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID
		FROM
			WORK.RANK_SUM;
QUIT;
RUN;
PROC SORT DATA=WORK.QCS_PREP_PREV_R6_2_SCORE;
BY DESCENDING SCORE;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 6.2 *
*---------------------------------------------*;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_2_SCORE 
               	where trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_2_SCORE
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR,
            AJUSTADOR,
			FREC_AJUS,
			NUM_BENEF,
			COUNT_MIN,
			COUNT_MAX,
			RANK1_FREC_AJUS,
			RANK2_NUM_BENEF,
			RANK3_COUNT_MIN,
			RANK4_COUNT_MAX,
			RANK_SUM,
            SCORE,
            BATCHDATE,
            SYSUSERID,
		   DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R6_2_SCORE;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_2_SCORE 
             where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;