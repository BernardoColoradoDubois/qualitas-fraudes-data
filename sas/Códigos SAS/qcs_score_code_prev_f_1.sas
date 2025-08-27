***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 7, 2021                                                                          *
* TITULO: Creación de Score General para la Familia 1                                             *
* OBJETIVO: Determinar Score General para la familia 1 con las reglas 1,2,3,4,5 y 6               *
* ENTRADAS: QCS_PREP_PREV_R1_1_SCORE, QCS_PREP_PREV_R1_2_SCORE, QCS_PREP_PREV_R1_3_SCORE          *
*           QCS_PREP_PREV_R1_4_SCORE, QCS_PREP_PREV_R1_5_SCORE, QCS_PREP_PREV_R1_6_SCORE          *
* SALIDAS: QCS_PREP_PREV_F1_SCORE                                                                 *
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

*--------------------------------------------------------*
* Asignación de importancia a las reglas de la familia 1 *
*--------------------------------------------------------*;
DATA WORK.RULES_RANK;
   INFILE DATALINES DELIMITER=',';
   LENGTH REGLA $10;
   INPUT REGLA $ NIVEL;
   DATALINES;
REGLA_1_2,6
REGLA_1_6,5
REGLA_1_3,4
REGLA_1_1,3
REGLA_1_4,2
REGLA_1_5,1
;
*--------------------------------------------------------*
* Asignación de porcentaje a las reglas de la familia 1 *
*--------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.RULES_RANK_PCT AS
		SELECT
            REGLA,
            NIVEL,
			DIVIDE(t1.NIVEL, SUM(t1.NIVEL)) AS PCT_NIVEL
		FROM
			WORK.RULES_RANK t1
	;
QUIT;
RUN;
*----------------------------------------------------------------*
* Transposición de los porcentajes de las reglas de la familia 1 *
*----------------------------------------------------------------*;
PROC TRANSPOSE DATA=WORK.RULES_RANK_PCT OUT=WORK.RULES_RANK_PCT_TR;
  ID REGLA;
 VAR PCT_NIVEL;
RUN;

*-------------------------------------------------------------------------------------------------------------*
* Proceso de validación de periodo por cada regla, se valida que los periodos maximos sean iguales 
*-------------------------------------------------------------------------------------------------------------*;
/*Obtiene fechas máxima de cada Score*/
proc sql noprint;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r1   from  &LIBN_SAS..QCS_PREP_PREV_R1_1_SCORE;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r2 from  &LIBN_SAS..QCS_PREP_PREV_R1_2_SCORE;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r3 from  &LIBN_SAS..QCS_PREP_PREV_R1_3_SCORE;   
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r4 from  &LIBN_SAS..QCS_PREP_PREV_R1_4_SCORE;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r5 from  &LIBN_SAS..QCS_PREP_PREV_R1_5_SCORE;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r6 from  &LIBN_SAS..QCS_PREP_PREV_R1_6_SCORE;
quit;

%let comma_=%str(%');
%put &=fec_max_r1;
%put &=fec_max_r2;
%put &=fec_max_r3;
%put &=fec_max_r4;
%put &=fec_max_r5;
%put &=fec_max_r6;
%let valida_periodo=0;
%let fecha_max=;

data valida_periodos;
    fec_fin=&fec_fin;
	regla1="&fec_max_r1"d;
	regla2="&fec_max_r2"d;
	regla3="&fec_max_r3"d;
	regla4="&fec_max_r4"d; 
	regla5="&fec_max_r5"d; 
	regla6="&fec_max_r6"d; 

	if fec_fin=regla2 and fec_fin=regla2 and fec_fin=regla3 and fec_fin=regla4 and fec_fin=regla5 and fec_fin=regla6 then
		do;
			call symput("valida_periodo", 1);
            call symput("fecha_max", cats("&comma_", put(&fec_fin,date9.),"&comma_",'d'));
             put 'Fecha maxima de periodos SI corresponden al mismo periodo, todas la reglas alertaron';
             put regla1=   regla2=  regla3=  regla4= regla5= regla6=;
		end;
	else
		do;
			call symput("valida_periodo", 1);
            call symput("fecha_max", &fec_fin);
			put 'Fecha maxima de periodos no corresponden al mismo periodo, No todas las reglas alertaron';
            put regla1=   regla2=  regla3=  regla4= regla5= regla6=;
            put 'Se toma la fecha del mes anterior a la fecha actual ' fec_fin=;

		end;
	format fec_fin regla1 regla2 regla3 regla4 regla5 regla6 date9.;
run;

%macro calculo_score_grl(); 
*------------------------------------------------------------------------------*
* Recupera el porcentaje de score de los agentes en cada regla de la familia 1 *
*------------------------------------------------------------------------------*;
PROC SQL;
CREATE TABLE WORK.QCS_PREP_PREV_R1_1_SCORE AS
   SELECT t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE, SCORE AS SCORE_1_1, BATCHDATE AS BATCHDATE_, PERIOD_BATCHDATE AS PERIOD_BATCHDATE_ FROM &LIBN_SAS..QCS_PREP_PREV_R1_1_SCORE t1
   
   LEFT JOIN 
       (SELECT DISTINCT CLAVE_UNICA_AGENTE, CVE_AGTE, NOM_AGTE FROM &LIBN_SAS..QCS_PREP_PREV_R1_1_ENGINE) t2 ON (t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE)
   
   HAVING t1.PERIOD_BATCHDATE = DHMS(&fecha_max.,0,0,0)
   ORDER BY t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE;
QUIT;
RUN;

PROC SQL;
CREATE TABLE WORK.QCS_PREP_PREV_R1_2_SCORE AS
   SELECT t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE, SCORE AS SCORE_1_2, BATCHDATE AS BATCHDATE_, PERIOD_BATCHDATE AS PERIOD_BATCHDATE_ FROM &LIBN_SAS..QCS_PREP_PREV_R1_2_SCORE t1
   
   LEFT JOIN 
       (SELECT DISTINCT CLAVE_UNICA_AGENTE, CVE_AGTE, NOM_AGTE FROM &LIBN_SAS..QCS_PREP_PREV_R1_2_ENGINE) t2 ON (t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE)
   
   HAVING t1.PERIOD_BATCHDATE = DHMS(&fecha_max.,0,0,0)
   ORDER BY t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE;
QUIT;
RUN;

PROC SQL;
CREATE TABLE WORK.QCS_PREP_PREV_R1_3_SCORE AS
   SELECT t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE, SCORE AS SCORE_1_3, BATCHDATE AS BATCHDATE_, PERIOD_BATCHDATE AS PERIOD_BATCHDATE_ FROM &LIBN_SAS..QCS_PREP_PREV_R1_3_SCORE t1
   
   LEFT JOIN 
       (SELECT DISTINCT CLAVE_UNICA_AGENTE, CVE_AGTE, NOM_AGTE FROM &LIBN_SAS..QCS_PREP_PREV_R1_3_ENGINE) t2 ON (t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE)
   
   HAVING t1.PERIOD_BATCHDATE = DHMS(&fecha_max.,0,0,0)
   ORDER BY t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE;
QUIT;
RUN;

PROC SQL;
CREATE TABLE WORK.QCS_PREP_PREV_R1_4_SCORE AS
   SELECT t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE, SCORE AS SCORE_1_4, BATCHDATE AS BATCHDATE_, PERIOD_BATCHDATE AS PERIOD_BATCHDATE_ FROM &LIBN_SAS..QCS_PREP_PREV_R1_4_SCORE t1
   
   LEFT JOIN 
       (SELECT DISTINCT CLAVE_UNICA_AGENTE, AGENTE AS CVE_AGTE, NOM_AGTE FROM &LIBN_SAS..QCS_PREP_PREV_R1_4_ENGINE) t2 ON (t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE)
   
   HAVING t1.PERIOD_BATCHDATE = DHMS(&fecha_max.,0,0,0)
   ORDER BY t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE;
QUIT;
RUN;

PROC SQL;
CREATE TABLE WORK.QCS_PREP_PREV_R1_5_SCORE AS
   SELECT t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE, SCORE AS SCORE_1_5, BATCHDATE AS BATCHDATE_, PERIOD_BATCHDATE AS PERIOD_BATCHDATE_ FROM &LIBN_SAS..QCS_PREP_PREV_R1_5_SCORE t1
   
   LEFT JOIN 
       (SELECT DISTINCT CLAVE_UNICA_AGENTE, AGENTE AS CVE_AGTE, NOM_AGTE FROM &LIBN_SAS..QCS_PREP_PREV_R1_5_ENGINE) t2 ON (t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE)
   
   HAVING t1.PERIOD_BATCHDATE = DHMS(&fecha_max.,0,0,0)
   ORDER BY t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE;
QUIT;
RUN;

PROC SQL;
CREATE TABLE WORK.QCS_PREP_PREV_R1_6_SCORE AS
   SELECT t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE, SCORE AS SCORE_1_6, BATCHDATE AS BATCHDATE_, PERIOD_BATCHDATE AS PERIOD_BATCHDATE_ FROM &LIBN_SAS..QCS_PREP_PREV_R1_6_SCORE t1
   
   LEFT JOIN 
       (SELECT DISTINCT CLAVE_UNICA_AGENTE, CVE_AGTE, NOM_AGTE FROM &LIBN_SAS..QCS_PREP_PREV_R1_6_ENGINE) t2 ON (t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE)
   
   HAVING t1.PERIOD_BATCHDATE = DHMS(&fecha_max.,0,0,0)
   ORDER BY t1.CLAVE_UNICA_AGENTE, t2.CVE_AGTE, t2.NOM_AGTE;
QUIT;
RUN;

DATA WORK.QCS_PREP_PREV_R1_SCORE;
 MERGE WORK.QCS_PREP_PREV_R1_1_SCORE
       WORK.QCS_PREP_PREV_R1_2_SCORE
       WORK.QCS_PREP_PREV_R1_3_SCORE
       WORK.QCS_PREP_PREV_R1_4_SCORE
       WORK.QCS_PREP_PREV_R1_5_SCORE
       WORK.QCS_PREP_PREV_R1_6_SCORE;
 BY CLAVE_UNICA_AGENTE CVE_AGTE NOM_AGTE;
RUN;
*---------------------------------------------------------------------------------------*
* Obtiene el porcentaje de score general de las reglas para cada agente de la familia 1 *
*---------------------------------------------------------------------------------------*;
DATA WORK.SCORE_GENERAL_F1;
 SET WORK.QCS_PREP_PREV_R1_SCORE;

   IF _N_ EQ 1 THEN DO;
      SET WORK.RULES_RANK_PCT_TR (DROP=_NAME_);
   END;

   F1_REGLA_1_1=SCORE_1_1 * REGLA_1_1;
   F1_REGLA_1_2=SCORE_1_2 * REGLA_1_2;
   F1_REGLA_1_3=SCORE_1_3 * REGLA_1_3;
   F1_REGLA_1_4=SCORE_1_4 * REGLA_1_4;
   F1_REGLA_1_5=SCORE_1_5 * REGLA_1_5;
   F1_REGLA_1_6=SCORE_1_6 * REGLA_1_6;

   SCORE=SUM(F1_REGLA_1_1, F1_REGLA_1_2, F1_REGLA_1_3, F1_REGLA_1_4, F1_REGLA_1_5, F1_REGLA_1_6);

   LENGTH SYSUSERID $10. BATCHDATE PERIOD_BATCHDATE 8.;

   SYSUSERID="&SYSUSERID.";

   BATCHDATE=BATCHDATE_;
   PERIOD_BATCHDATE=PERIOD_BATCHDATE_;
 
   DROP BATCHDATE_ PERIOD_BATCHDATE_;
RUN;
*-------------------------------------------------------------------*
* Asigna el valor cero a los porcentajes que no obtuvieron un valor *
*-------------------------------------------------------------------*;
DATA WORK.QCS_PREP_PREV_F1_SCORE;
 SET WORK.SCORE_GENERAL_F1;
  DROP _ROW_;
  ARRAY _VARS_(*) _NUMERIC_;
	DO _ROW_=1 TO DIM(_VARS_);
	 IF _VARS_(_ROW_)=. THEN _VARS_(_ROW_)=0;
	END;
RUN;
*--------------------------------------------------------*
* Ordena por el score general las reglas de la familia 1 *
*--------------------------------------------------------*;
PROC SORT DATA=WORK.QCS_PREP_PREV_F1_SCORE
(KEEP=CLAVE_UNICA_AGENTE CVE_AGTE NOM_AGTE F1_REGLA_1_1 F1_REGLA_1_2 F1_REGLA_1_3 F1_REGLA_1_4 F1_REGLA_1_5 F1_REGLA_1_6 SCORE SYSUSERID BATCHDATE PERIOD_BATCHDATE);
  BY DESCENDING SCORE;
RUN;
*-----------------------------------------------------*
* Genera la tabla del score general para la familia 1 *
*-----------------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_F1_SCORE 
          where trunc(PERIOD_BATCHDATE) =TRUNC(TO_DATE(&fec_fin_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_F1_SCORE
		SELECT 
            CLAVE_UNICA_AGENTE,
			CVE_AGTE,
			NOM_AGTE,
			F1_REGLA_1_1,
            F1_REGLA_1_2,
            F1_REGLA_1_3,
            F1_REGLA_1_4,
            F1_REGLA_1_5,
            F1_REGLA_1_6,
            SCORE,
            SYSUSERID,
            BATCHDATE,
			PERIOD_BATCHDATE           
		FROM
			WORK.QCS_PREP_PREV_F1_SCORE;
QUIT;
RUN;

%mend;

%macro genera_calculo_score();
%put &=valida_periodo;
%put &=fecha_max;

   %if  %eval(&valida_periodo = 1 ) %then %do;
        %calculo_score_grl();
   %end;   
   %else %do;
     %put NOTE: peridos diferentes, validar periodo de ejecución de reglas;
   %end;
%mend;

%genera_calculo_score();
Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;