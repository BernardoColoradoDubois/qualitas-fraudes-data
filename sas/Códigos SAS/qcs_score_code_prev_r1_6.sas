***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 4, 2021                                                                          *
* TITULO: Creación de Score para la Regla 1.6                                                     *
* OBJETIVO: Determinar Score para la familia 1 en la regla 1.6                                    *
* ENTRADAS: QCS_PREP_PREV_R1_6_RESULT                                                             *
* SALIDAS: QCS_PREP_PREV_R1_6_SCORE                                                               *
***************************************************************************************************;
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

	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

*---------------------------------------------------*
* Obtiene la mediana de las polizas vigentes agente *
*---------------------------------------------------*;
PROC SQL NOPRINT;
    SELECT MEDIAN(CNT_POL_VIG)
     INTO :MEDIAN_CNT_POL_VIG
        FROM &LIBN_SAS..QCS_PREP_PREV_R1_6_RESULT;
QUIT;
RUN;
*------------------------------------*
* Calcular el score por agente unico *
*------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R1_6_SCORE AS
		SELECT 
            CLAVE_UNICA_AGENTE,
            CNT_POL_DIF_EDO_EMI,
			CNT_POL_VIG,  /* contiene todas las polizas sin importar el estatus */
            PCT_POL_DIF_EDO_EMI as SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R1_6_RESULT
        WHERE PERIOD_BATCHDATE = DHMS(&fec_fin.,0,0,0) 
              and  missing(CLAVE_UNICA_AGENTE) eq 0
         AND CNT_POL_VIG > &MEDIAN_CNT_POL_VIG. 
    ORDER BY PCT_POL_DIF_EDO_EMI DESC;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 1.6 *
*---------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R1_6_SCORE 
          where trunc(PERIOD_BATCHDATE) =TRUNC(TO_DATE(&fec_fin_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R1_6_SCORE
		SELECT 
            CLAVE_UNICA_AGENTE,
			CNT_POL_DIF_EDO_EMI,
			CNT_POL_VIG,
            SCORE,
            BATCHDATE,
            SYSUSERID,
			PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R1_6_SCORE;
QUIT;
RUN;

*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
   execute(delete from QCS_PREP_PREV_R1_6_SCORE 
           where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;