***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 4, 2021                                                                          *
* TITULO: Creación de Score para la Regla 1.2                                                     *
* OBJETIVO: Determinar Score para la familia 1 en la regla 1.2                                    *
* ENTRADAS: QCS_PREP_PREV_R1_2_RESULT                                                             *
* SALIDAS: QCS_PREP_PREV_R1_2_SCORE                                                               *
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

*----------------------------------------------*
* Genera las metricas requeridas para el score *
*----------------------------------------------*;
Proc sql;
    create table WORK.SCORE_T1 as
	select
		t1.CLAVE_UNICA_AGENTE,
        t1.CNT_POL_AGTE,
		 case when t1.CNT_POL_CAN_AUT is not missing then t1.CNT_POL_CAN_AUT 
         else 0
         end as CNT_POL_CAN_AUT,
        range(
              case when t1.CNT_POL_CANCELADA is not missing
                   then t1.CNT_POL_CANCELADA 
                   else 0 end, 
              case when t1.CNT_POL_CAN_AUT is not missing
                    then t1.CNT_POL_CAN_AUT 
                    else 0 end
               ) as CNT_POL_CAN_MANUAL,
        (divide(calculated CNT_POL_CAN_AUT, t1.CNT_POL_AGTE) * 0.70) as PCT_SCORE_POL_CAN_AUT,
        (divide(calculated CNT_POL_CAN_MANUAL, t1.CNT_POL_AGTE) * 0.30) as PCT_SCORE_POL_CAN_MANUAL,
        sum(calculated PCT_SCORE_POL_CAN_AUT, calculated PCT_SCORE_POL_CAN_MANUAL) as PCT_SCORE,
        DATETIME() as BATCHDATE format=DATETIME20.,
        "&SYSUSERID." AS SYSUSERID length=10,
        PERIOD_BATCHDATE
	from &LIBN_SAS..QCS_PREP_PREV_R1_2_RESULT t1
    WHERE datepart(PERIOD_BATCHDATE) = &fec_fin.
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

%put &=MAX_PCT_SCORE;
*-----------------------------------*
* Calcula el score por agente unico *
*-----------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R1_2_SCORE AS
		SELECT 
            t1.CLAVE_UNICA_AGENTE,
            t1.CNT_POL_AGTE,
		    t1.CNT_POL_CAN_AUT,
            t1.CNT_POL_CAN_MANUAL,
            t1.PCT_SCORE_POL_CAN_AUT,
            t1.PCT_SCORE_POL_CAN_MANUAL,
            t1.PCT_SCORE,
            DIVIDE(t1.PCT_SCORE,&MAX_PCT_SCORE.) AS SCORE,
            BATCHDATE,
            SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			WORK.SCORE_T1 t1
    ORDER BY calculated SCORE DESC, t1.CNT_POL_AGTE DESC;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 1.2 *
*---------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R1_2_SCORE 
          where trunc(PERIOD_BATCHDATE) =TRUNC(TO_DATE(&fec_fin_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R1_2_SCORE
		SELECT 
            CLAVE_UNICA_AGENTE,
            CNT_POL_AGTE,
		    CNT_POL_CAN_AUT,
            CNT_POL_CAN_MANUAL,
            PCT_SCORE_POL_CAN_AUT,
            PCT_SCORE_POL_CAN_MANUAL,
            PCT_SCORE,
            SCORE,
            BATCHDATE,
            SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R1_2_SCORE;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
   execute(delete from QCS_PREP_PREV_R1_2_SCORE 
           where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;