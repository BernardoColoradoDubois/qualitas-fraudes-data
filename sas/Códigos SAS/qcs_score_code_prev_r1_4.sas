***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 4, 2021                                                                          *
* TITULO: Creación de Score para la Regla 1.4                                                     *
* OBJETIVO: Determinar Score para la familia 1 en la regla 1.4                                    *
* ENTRADAS: QCS_PREP_PREV_R1_4_RESULT                                                             *
* SALIDAS: QCS_PREP_PREV_R1_4_SCORE                                                               *
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
  
/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

*----------------------------------------------*
* Genera las metricas requeridas para el score *
*----------------------------------------------*;
Proc sql;
    create table WORK.SCORE_T1 as
	select
		t1.CLAVE_UNICA_AGENTE,
        case when t1.CNT_POL_CANCELA_AUT_SINPAGO is not missing then t1.CNT_POL_CANCELA_AUT_SINPAGO else 0 end as CNT_POL_CANCELA_AUT_SINPAGO,
		case when t1.CNT_POL_CANCELA_USR_SINPAGO is not missing then t1.CNT_POL_CANCELA_USR_SINPAGO else 0 end as CNT_POL_CANCELA_USR_SINPAGO,
		case when t1.CNT_POL_CANCELA_AUT_CONPAGO is not missing then t1.CNT_POL_CANCELA_AUT_CONPAGO else 0 end as CNT_POL_CANCELA_AUT_CONPAGO,
        case when t1.CNT_POL_CANCELA_USR_CONPAGO is not missing then t1.CNT_POL_CANCELA_USR_CONPAGO else 0 end as CNT_POL_CANCELA_USR_CONPAGO,
        /*SUM(t1.CNT_POL_CANCELADAS, t1.CNT_POL_VIG) AS TOTAL_POL_AGENTE, */
        t1.CNT_POL_VIG AS TOTAL_POL_AGENTE,  /*cotiene el conteo de polizas unicas sin importar el estatus*/
        SUM(calculated CNT_POL_CANCELA_AUT_SINPAGO, calculated CNT_POL_CANCELA_AUT_CONPAGO) AS TOT_POL_CANCELA_AUT,
        SUM(calculated CNT_POL_CANCELA_USR_SINPAGO, calculated CNT_POL_CANCELA_USR_CONPAGO) AS TOT_POL_CANCELA_USR,
        (DIVIDE(calculated TOT_POL_CANCELA_AUT, CNT_POL_VIG) * 0.70) AS PCT_SCORE_POL_CAN_AUT,
        (DIVIDE(calculated TOT_POL_CANCELA_USR, CNT_POL_VIG) * 0.30) AS PCT_SCORE_POL_CAN_USR,
        SUM(calculated PCT_SCORE_POL_CAN_AUT, calculated PCT_SCORE_POL_CAN_USR) as PCT_SCORE,
        DATETIME() as BATCHDATE format=DATETIME20.,
        "&SYSUSERID." AS SYSUSERID,
        PERIOD_BATCHDATE
	from &LIBN_SAS..QCS_PREP_PREV_R1_4_RESULT t1
    	WHERE PERIOD_BATCHDATE = DHMS(&fec_fin.,0,0,0)
			and  missing(CLAVE_UNICA_AGENTE) eq 0;
Quit;
*--------------------------------------------*
* Obtiene el maximo del Porcentaje del Score *
*--------------------------------------------*;
PROC SQL NOPRINT;
    SELECT MAX(PCT_SCORE)
     INTO :MAX_PCT_SCORE
        FROM WORK.SCORE_T1;
QUIT;
RUN;
*-----------------------------------*
* Calcula el score por agente unico *
*-----------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R1_4_SCORE AS
		SELECT 
            t1.CLAVE_UNICA_AGENTE,
            t1.TOTAL_POL_AGENTE,
            t1.TOT_POL_CANCELA_AUT,
            t1.TOT_POL_CANCELA_USR,
            t1.PCT_SCORE_POL_CAN_AUT,
            t1.PCT_SCORE_POL_CAN_USR,
            t1.PCT_SCORE,
            DIVIDE(t1.PCT_SCORE,&MAX_PCT_SCORE.) AS SCORE,
            BATCHDATE,
            SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			WORK.SCORE_T1 t1
    ORDER BY calculated SCORE DESC, t1.TOTAL_POL_AGENTE DESC;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 1.4 *
*---------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R1_4_SCORE 
          where trunc(PERIOD_BATCHDATE) =TRUNC(TO_DATE(&fec_fin_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R1_4_SCORE
		SELECT 
            CLAVE_UNICA_AGENTE,
            TOTAL_POL_AGENTE,
            TOT_POL_CANCELA_AUT,
            TOT_POL_CANCELA_USR,
            PCT_SCORE_POL_CAN_AUT,
            PCT_SCORE_POL_CAN_USR,
            PCT_SCORE,
            SCORE,
            BATCHDATE,
            SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R1_4_SCORE;
QUIT;
RUN;

*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
   execute(delete from QCS_PREP_PREV_R1_4_SCORE 
           where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;


Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;