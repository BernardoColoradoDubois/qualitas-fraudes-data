***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 12, 2021                                                                         *
* TITULO: Creación de Score para la Regla 6.4                                                     *
* OBJETIVO: Determinar Score para la familia 6 en la regla 6.4                                    *
* ENTRADAS: QCS_PREP_PREV_R6_4_ENGINE, QCS_PREP_PREV_R6_4_DETAIL                                  *
* SALIDAS: QCS_PREP_PREV_R6_4_SCORE                                                               *
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


/*Filtra el último periodo cargado en la tabla DETAIL*/
data tmp_QCS_PREP_PREV_R6_4_DETAIL;
    set &LIBN_SAS..QCS_PREP_PREV_R6_4_DETAIL;
   if datepart(PERIOD_BATCHDATE) =  datepart(DHMS(&FEC_FIN.,0,0,0));
run;

*---------------------------------------------------*
* Obtiene la frecuencia de siniestros por ajustador *
*---------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.DIAS_OCU_DEP AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.NOMBRE_AJUSTADOR_GR,
            t1.AJUSTADOR,
            t1.NOMBRE_AJUSTADOR,
			COUNT(DISTINCT t1.SINIESTRO) AS DIAS_OCU_DEP
		FROM  tmp_QCS_PREP_PREV_R6_4_DETAIL t1
	GROUP BY
		t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;

proc sort data=DIAS_OCU_DEP nodupkey;
  by CVE_NOMBRE_AJUSTADOR_GR;
quit;


*--------------------------------------------------------*
* Obtiene el numero total de siniestros por el ajustador *  
*--------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.N_TOTAL AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			COUNT(DISTINCT t1.SINIESTRO) AS N_TOTAL
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_4_ENGINE  t1
	GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;
*-------------------------------------------------------*
* Obtiene el porcentaje de incumplimiento del ajustador *
*-------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.PCT_SINIESTRO AS
		SELECT
		    t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.NOMBRE_AJUSTADOR_GR,
            t1.AJUSTADOR,
            t1.NOMBRE_AJUSTADOR,
			t1.DIAS_OCU_DEP,
			t2.N_TOTAL,
            DIVIDE(t1.DIAS_OCU_DEP,t2.N_TOTAL) AS PCT_SINIESTRO
		FROM
			WORK.DIAS_OCU_DEP t1
				LEFT JOIN WORK.N_TOTAL t2 ON (t1.CVE_NOMBRE_AJUSTADOR_GR =t2.CVE_NOMBRE_AJUSTADOR_GR)
	;
QUIT;
RUN;
*--------------------------------------------------------------------------------*
* Obtiene el numero minimo y maximo de dias que demora el ajustador en depositar *
*--------------------------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.MIN_MAX AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			MIN(t1.DIAS_OCURRE_DEP) AS DIAS_OCU_DEP_MIN,
            MAX(t1.DIAS_OCURRE_DEP) AS DIAS_OCU_DEP_MAX
		FROM
			tmp_QCS_PREP_PREV_R6_4_DETAIL t1
	GROUP BY t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;  
*---------------------------------------------------------------------------------------*
* Obtiene la media aritmetica y la mediana del porcentaje de recuperación del ajustador *
*---------------------------------------------------------------------------------------*;
PROC MEANS DATA= tmp_QCS_PREP_PREV_R6_4_DETAIL N MEAN MEDIAN NOPRINT NWAY;
VAR DIAS_OCURRE_DEP;
CLASS CVE_NOMBRE_AJUSTADOR_GR;
OUTPUT OUT=WORK.RESULT (DROP= _TYPE_ _FREQ_)
N =
MEAN =
MEDIAN = / AUTONAME;
RUN;
*-----------------------------------------*
* Obtiene el comportamiento del ajustador *
*-----------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COMP_AJUS AS
		SELECT
			t1.*,
			(DIVIDE(t1.DIAS_OCURRE_DEP_Mean - t1.DIAS_OCURRE_DEP_Median, t1.DIAS_OCURRE_DEP_Median )) AS COMP_AJUS
		FROM
			WORK.RESULT t1
	;
QUIT;
RUN;
*---------------------------------------------------*
* Obtiene el monto total ingresado por el ajustador *
*---------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.MONTO_INGRESO_SUM AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			SUM(t1.MONTO_INGRESO) AS MONTO_INGRESO_SUM
		FROM
			tmp_QCS_PREP_PREV_R6_4_DETAIL t1
	GROUP BY
		t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;
*--------------------------------------*
* Obtiene el consolidado del ajustador *
*--------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.CONSOLIDADO AS
		SELECT
            t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.NOMBRE_AJUSTADOR_GR,
			t1.AJUSTADOR,
            t1.NOMBRE_AJUSTADOR,
			t1.N_TOTAL,
			t1.DIAS_OCU_DEP,
            t1.PCT_SINIESTRO,
            t2.DIAS_OCU_DEP_MIN,
            t2.DIAS_OCU_DEP_MAX,
            t3.COMP_AJUS,
            t4.MONTO_INGRESO_SUM
		FROM
			WORK.PCT_SINIESTRO t1
				LEFT JOIN WORK.MIN_MAX t2 ON (t1.CVE_NOMBRE_AJUSTADOR_GR = t2.CVE_NOMBRE_AJUSTADOR_GR)
                LEFT JOIN WORK.COMP_AJUS t3 ON (t1.CVE_NOMBRE_AJUSTADOR_GR = t3.CVE_NOMBRE_AJUSTADOR_GR)
                LEFT JOIN WORK.MONTO_INGRESO_SUM t4 ON (t1.CVE_NOMBRE_AJUSTADOR_GR = t4.CVE_NOMBRE_AJUSTADOR_GR)
	;
QUIT;
RUN;
*------------------------------------*
* Asigna los rangos a los parametros *
*------------------------------------*;
PROC RANK DATA=WORK.CONSOLIDADO OUT=WORK.RANKS TIES=MEAN DESCENDING;
   VAR DIAS_OCU_DEP PCT_SINIESTRO DIAS_OCU_DEP_MIN DIAS_OCU_DEP_MAX COMP_AJUS MONTO_INGRESO_SUM;
   RANKS RANK1_DIAS_OCU_DEP RANK2_PCT_SINIESTRO RANK3_DIAS_OCU_DEP_MIN RANK4_DIAS_OCU_DEP_MAX RANK5_COMP_AJUS RANK6_MONTO_INGRESO_SUM;
RUN;
*-------------------------------*
* Obtiene la suma de los rangos *
*-------------------------------*;
PROC SQL;
	CREATE TABLE WORK.RANK_SUM AS
		SELECT
			t1.*,
			SUM(t1.RANK1_DIAS_OCU_DEP, t1.RANK2_PCT_SINIESTRO, t1.RANK3_DIAS_OCU_DEP_MIN, t1.RANK4_DIAS_OCU_DEP_MAX, t1.RANK5_COMP_AJUS, t1.RANK6_MONTO_INGRESO_SUM ) AS RANK_SUM
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
*---------------------------------*
* Calcular el score por ajustador *
*---------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R6_4_SCORE AS
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR,
            NOMBRE_AJUSTADOR_GR,
            AJUSTADOR,
            NOMBRE_AJUSTADOR,
			N_TOTAL,
			DIAS_OCU_DEP,
            PCT_SINIESTRO,
            DIAS_OCU_DEP_MIN,
            DIAS_OCU_DEP_MAX,
            COMP_AJUS,
            MONTO_INGRESO_SUM,
			RANK1_DIAS_OCU_DEP,
            RANK2_PCT_SINIESTRO,
            RANK3_DIAS_OCU_DEP_MIN,
            RANK4_DIAS_OCU_DEP_MAX,
            RANK5_COMP_AJUS,
            RANK6_MONTO_INGRESO_SUM,
			RANK_SUM,
            DIVIDE(&MIN_RANK_SUM.,RANK_SUM) AS SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID
		FROM
			WORK.RANK_SUM
    ORDER BY CALCULATED SCORE DESC;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 6.4 *
*---------------------------------------------*;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_4_SCORE 
         	where trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_4_SCORE
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR,
            NOMBRE_AJUSTADOR_GR,
            AJUSTADOR,
            NOMBRE_AJUSTADOR,
			N_TOTAL,
			DIAS_OCU_DEP,
            PCT_SINIESTRO,
            DIAS_OCU_DEP_MIN,
            DIAS_OCU_DEP_MAX,
            COMP_AJUS,
            MONTO_INGRESO_SUM,
			RANK1_DIAS_OCU_DEP,
            RANK2_PCT_SINIESTRO,
            RANK3_DIAS_OCU_DEP_MIN,
            RANK4_DIAS_OCU_DEP_MAX,
            RANK5_COMP_AJUS,
            RANK6_MONTO_INGRESO_SUM,
			RANK_SUM,
            SCORE,
            BATCHDATE,
            SYSUSERID,
            DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R6_4_SCORE;
QUIT;
RUN;

*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_4_SCORE 
     where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))

) by oracle;
disconnect from oracle;
QUIT;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;