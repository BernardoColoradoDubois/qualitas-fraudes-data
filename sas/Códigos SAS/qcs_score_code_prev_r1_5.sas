***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 4, 2021                                                                          *
* TITULO: Creación de Score para la Regla 1.5                                                     *
* OBJETIVO: Determinar Score para la familia 1 en la regla 1.5                                    *
* ENTRADAS: QCS_PREP_PREV_R1_5_RESULT                                                             *
* SALIDAS: QCS_PREP_PREV_R1_5_SCORE                                                               *
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
preserve_tab_names=no preserve_col_names=no 
readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;

Libname &LIBN_SAS. oracle path=&Res_ora_path. schema="&Res_ora_schema." user="&Res_ora_user." password=&Res_ora_password.
preserve_tab_names=no preserve_col_names=no 
readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;
*-------------------------------------------------------------------*
* Obtiene el maximo de Polizas Ramos No Cancelables mayor a 45 dias *
*-------------------------------------------------------------------*;
PROC SQL NOPRINT;
    SELECT MAX(POL_VIG_RAMOS_NO_CANC_SINPAGO)
     INTO :MAX_RAMOS_NO_CANC
        FROM &LIBN_SAS..QCS_PREP_PREV_R1_5_RESULT;
QUIT;
RUN;
*------------------------------------*
* Calcular el score por agente unico *
*------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R1_5_SCORE AS
		SELECT 
            t1.CLAVE_UNICA_AGENTE,
			t1.POL_CANC_SINPAGO,
            t1.POL_VIG_RAMOS_NO_CANC_SINPAGO,
		    t1.TOTAL_POL_AGTE,
		    DIVIDE(t1.POL_VIG_RAMOS_NO_CANC_SINPAGO,&MAX_RAMOS_NO_CANC.) AS SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R1_5_RESULT t1
		    WHERE PERIOD_BATCHDATE = DHMS(&fec_fin.,0,0,0)
				and  missing(CLAVE_UNICA_AGENTE) eq 0
    ORDER BY t1.POL_VIG_RAMOS_NO_CANC_SINPAGO DESC, t1.TOTAL_POL_AGTE DESC, calculated SCORE DESC;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 1.5 *
*---------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R1_5_SCORE 
          where trunc(PERIOD_BATCHDATE) =TRUNC(TO_DATE(&fec_fin_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R1_5_SCORE
		SELECT 
            CLAVE_UNICA_AGENTE,
			POL_CANC_SINPAGO,
			POL_VIG_RAMOS_NO_CANC_SINPAGO,
			TOTAL_POL_AGTE,
            SCORE,
            BATCHDATE,
            SYSUSERID,
			PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R1_5_SCORE;
QUIT;
RUN;

*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
   execute(delete from QCS_PREP_PREV_R1_5_SCORE 
           where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;