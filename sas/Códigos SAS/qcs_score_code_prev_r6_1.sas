***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 8, 2021                                                                          *
* TITULO: Creación de Score para la Regla 6.1                                                     *
* OBJETIVO: Determinar Score para la familia 6 en la regla 6.1                                    *
* ENTRADAS: QCS_PREP_PREV_R6_1_ENGINE, QCS_PREP_PREV_R6_1_RESULT                                  *
* SALIDAS: QCS_PREP_PREV_R6_1_SCORE                                                               *
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


options sastrace=',,,d' sastraceloc=saslog nostsuffix;
*--------------------------------------------------------------*
* Obtiene el numero de siniestros incumplidos por el ajustador *
*--------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.N_RECUP AS
		SELECT
		    t1.CVE_NOMBRE_AJUSTADOR_GR,
			COUNT(DISTINCT t1.SINIESTRO) AS N_RECUP
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_1_RESULT t1
            where   flg_incumplido_ajustador = 1
	GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;
*--------------------------------------------------------*
* Obtiene el numero total de siniestros por el ajustador *
*--------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.N_TOTAL AS
		SELECT
			 t1.CVE_NOMBRE_AJUSTADOR_GR,
			COUNT(DISTINCT t1.SINIESTRO) AS N_TOTAL
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_1_ENGINE (WHERE=(CVE_NOMBRE_AJUSTADOR_GR IS NOT NULL)) t1
	GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;
*-------------------------------------------------------*
* Obtiene el porcentaje de incumplimiento del ajustador *
*-------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.PCT_INCUMPL AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			t1.N_RECUP,
			t2.N_TOTAL,
            DIVIDE(t1.N_RECUP,t2.N_TOTAL) AS PCT_INCUMPL
		FROM
			WORK.N_RECUP t1
				LEFT JOIN WORK.N_TOTAL t2 ON (t1.CVE_NOMBRE_AJUSTADOR_GR = t2.CVE_NOMBRE_AJUSTADOR_GR)
	;
QUIT;
RUN;
*----------------------------------------------*
* Obtiene el monto no recuperado del ajustador *
*----------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.MONTO_NO_RECUP AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			SUM(t1.TOTAL_VALUACION) AS TOTAL_VALUACION,
			SUM(t1.MONTO_INGRESO) AS MONTO_INGRESO,
			RANGE(calculated TOTAL_VALUACION, calculated MONTO_INGRESO) AS MONTO_NO_RECUP
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_1_RESULT (WHERE=(flg_incumplido_ajustador = 1)) t1
	GROUP BY
		t1.CVE_NOMBRE_AJUSTADOR_GR;
QUIT;
RUN;
*-----------------------------------------------------*
* Obtiene el porcentaje de recuperacion del ajustador *
*-----------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.PCT_RECUP AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			DIVIDE(t1.MONTO_INGRESO, t1.TOTAL_VALUACION) AS PCT_RECUP
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R6_1_RESULT (WHERE=(flg_incumplido_ajustador = 1)) t1
	;
QUIT;
RUN;
*---------------------------------------------------------------------------------------*
* Obtiene la media aritmetica y la mediana del porcentaje de recuperacion del ajustador *
*---------------------------------------------------------------------------------------*;
PROC MEANS DATA=WORK.PCT_RECUP N MEAN MEDIAN NOPRINT NWAY;
VAR PCT_RECUP;
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
			DIVIDE(t1.PCT_RECUP_Mean - t1.PCT_RECUP_Median, t1.PCT_RECUP_Median ) AS COMP_AJUS
		FROM
			WORK.RESULT t1
	;
QUIT;
RUN;
*--------------------------------------*
* Obtiene el consolidado del ajustador *
*--------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.CONSOLIDADO AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			t1.N_TOTAL,
			t1.N_RECUP,
            t1.PCT_INCUMPL,
            t2.MONTO_NO_RECUP,
            t3.COMP_AJUS
		FROM
			WORK.PCT_INCUMPL t1
				LEFT JOIN WORK.MONTO_NO_RECUP t2 ON (t1.CVE_NOMBRE_AJUSTADOR_GR   =t2.CVE_NOMBRE_AJUSTADOR_GR  )
                LEFT JOIN WORK.COMP_AJUS t3 ON (t1.CVE_NOMBRE_AJUSTADOR_GR=t3.CVE_NOMBRE_AJUSTADOR_GR)
	;
QUIT;
RUN;
*------------------------------------*
* Asigna los rangos a los parametros *
*------------------------------------*;
PROC RANK DATA=WORK.CONSOLIDADO OUT=WORK.RANKS DESCENDING;
   VAR N_RECUP PCT_INCUMPL MONTO_NO_RECUP COMP_AJUS;
   RANKS RANK1_N_RECUP RANK2_PCT_INCUMPL RANK3_MONTO_NO_RECUP RANK4_COMP_AJUS;
RUN;
*-------------------------------*
* Obtiene la suma de los rangos *
*-------------------------------*;
PROC SQL;
	CREATE TABLE WORK.RANK_SUM AS
		SELECT
			t1.*,
			SUM(t1.RANK1_N_RECUP, t1.RANK2_PCT_INCUMPL, t1.RANK3_MONTO_NO_RECUP, t1.RANK4_COMP_AJUS ) AS RANK_SUM
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
	CREATE TABLE WORK.BATCHDATE AS
		SELECT MAX(PERIOD_BATCHDATE) AS PERIOD_BATCHDATE 
		FROM &LIBN_SAS..QCS_PREP_PREV_R6_1_RESULT  (WHERE=( flg_incumplido_ajustador = 1 ) );
QUIT;

PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R6_1_SCORE AS
		SELECT 
            t1.CVE_NOMBRE_AJUSTADOR_GR, 
			t1.N_TOTAL,
			t1.N_RECUP,
			t1.PCT_INCUMPL,
			t1.MONTO_NO_RECUP,
			t1.COMP_AJUS,
			t1.RANK1_N_RECUP,
			t1.RANK2_PCT_INCUMPL,
			t1.RANK3_MONTO_NO_RECUP,
			t1.RANK4_COMP_AJUS,
			t1.RANK_SUM,
            DIVIDE(&MIN_RANK_SUM.,t1.RANK_SUM) AS SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID
		FROM
			WORK.RANK_SUM t1
    ORDER BY CALCULATED SCORE DESC;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 6.1 *
*---------------------------------------------*;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_1_SCORE 
         	where trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_1_SCORE
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR,
			N_TOTAL,
			N_RECUP,
			PCT_INCUMPL,
			MONTO_NO_RECUP,
			COMP_AJUS,
			RANK1_N_RECUP,
			RANK2_PCT_INCUMPL,
			RANK3_MONTO_NO_RECUP,
			RANK4_COMP_AJUS,
			RANK_SUM,
            SCORE,
            BATCHDATE,
            SYSUSERID,
            DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R6_1_SCORE;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_1_SCORE 
            where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;