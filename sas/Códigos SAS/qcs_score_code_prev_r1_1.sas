***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 4, 2021                                                                          *
* TITULO: Creación de Score para la Regla 1.1                                                     *
* OBJETIVO: Determinar Score para la familia 1 en la regla 1.1                                    *
* ENTRADAS: QCS_PREP_PREV_R1_1_RESULT                                                             *
* SALIDAS: QCS_PREP_PREV_R1_1_SCORE                                                               *
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

*----------------------------------------*
* Obtiene el maximo de unidades carrusel *
*----------------------------------------*;
PROC SQL NOPRINT;
    SELECT MAX(UNIDADES_CARRUSEL)
     INTO :MAX_CARRUSEL
        FROM &LIBN_SAS..QCS_PREP_PREV_R1_1_RESULT
      where   datepart(PERIOD_BATCHDATE) = &FEC_FIN. and missing(CLAVE_UNICA_AGENTE) = 0;
QUIT;
RUN;
%put &=MAX_CARRUSEL;

*------------------------------------*
* Calcular el score por agente unico *
*------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_R1_1_SCORE AS
		SELECT 
            CLAVE_UNICA_AGENTE,
			LARGO_CARRUSEL,
			UNIDADES_CARRUSEL,
            DIVIDE(UNIDADES_CARRUSEL,&MAX_CARRUSEL.) AS SCORE,
            DATETIME() as BATCHDATE format=DATETIME20.,
            "&SYSUSERID." AS SYSUSERID,
            DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R1_1_RESULT
             where  datepart(PERIOD_BATCHDATE) = &FEC_FIN. and  missing(CLAVE_UNICA_AGENTE) = 0
    ORDER BY UNIDADES_CARRUSEL DESC, LARGO_CARRUSEL DESC, CALCULATED SCORE DESC;
QUIT;
RUN;
*---------------------------------------------*
* Genera la tabla del score para la Regla 1.1 *
*---------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R1_1_SCORE 
          where trunc(PERIOD_BATCHDATE) =TRUNC(TO_DATE(&fec_fin_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R1_1_SCORE
		SELECT 
            CLAVE_UNICA_AGENTE,
			LARGO_CARRUSEL,
			UNIDADES_CARRUSEL,
            SCORE,
            BATCHDATE,
            SYSUSERID,
            PERIOD_BATCHDATE
		FROM
			WORK.QCS_PREP_PREV_R1_1_SCORE;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from  QCS_PREP_PREV_R1_1_SCORE
         where  trunc(PERIOD_BATCHDATE) <= TRUNC(TO_DATE(&fec_borra_ora, 'YYYY-MM-DD HH24:MI:SS'))
        ) by oracle;
disconnect from oracle;
QUIT;


Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;