***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Octubre 13, 2021                                                                         *
* TITULO: Creación de Score General para la Familia 6                                             *
* OBJETIVO: Determinar Score General para la familia 6 con las reglas 1,2,3 y 4                   *
* ENTRADAS: QCS_PREP_PREV_R6_1_SCORE, QCS_PREP_PREV_R6_2_SCORE, QCS_PREP_PREV_R6_3_SCORE          *
*           QCS_PREP_PREV_R6_4_SCORE                                                              *
* SALIDAS: QCS_PREP_PREV_F6_SCORE                                                                 *
***************************************************************************************************;
*------------------------*
* Parametros de sistema  *
*------------------------*;

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
   INPUT REGLA $ FACTOR;
   DATALINES;
REGLA_6_1,0.4
REGLA_6_2,0.2
REGLA_6_3,0.2
REGLA_6_4,0.2
;
*----------------------------------------------------------------*
* Transposición de los porcentajes de las reglas de la familia 1 *
*----------------------------------------------------------------*;
PROC TRANSPOSE DATA=WORK.RULES_RANK OUT=WORK.RULES_RANK_TR;
  ID REGLA;
 VAR FACTOR;
RUN;

*-------------------------------------------------------------------------------------------------------------*
* Proceso de validación de periodo por cada regla, se valida que los periodos maximos sean iguales 
*-------------------------------------------------------------------------------------------------------------*;
/*Obtiene fechas máxima de cada Score*/
proc sql noprint;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r1   from  &LIBN_SAS..QCS_PREP_PREV_R6_1_SCORE;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r2 from  &LIBN_SAS..QCS_PREP_PREV_R6_2_SCORE;
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r3 from  &LIBN_SAS..QCS_PREP_PREV_R6_3_SCORE;   
   select distinct MAX(datepart(PERIOD_BATCHDATE))   format=date9. into: fec_max_r4 from  &LIBN_SAS..QCS_PREP_PREV_R6_4_SCORE;
quit;

%let comma_=%str(%');
%put &=fec_max_r1;
%put &=fec_max_r2;
%put &=fec_max_r3;
%put &=fec_max_r4;
%let valida_periodo=0;
%let fecha_max=;


data valida_periodos;
	regla1="&fec_max_r1"d;
	regla2="&fec_max_r2"d;
	regla3="&fec_max_r3"d;
	regla4="&fec_max_r4"d; 
   	periodo=&fec_fin.; 

	if  regla1=periodo and regla2=periodo and  regla3=periodo  and regla4=periodo  then
		do;
			call symput("valida_periodo", 1);
            call symput("fecha_max", cats("&comma_", put(periodo,date9.),"&comma_",'d'));
            put 'Fecha maxima de periodos SI corresponden al mismo periodo, todas la reglas alertaron';
            put periodo=  regla1=   regla2=  regla3=  regla4=;
		end;
	else
		do;
			call symput("valida_periodo", 1);
			put 'Fecha maxima de periodos no corresponden al mismo periodo, No todas las reglas alertaron';
            put periodo=  regla1=   regla2=  regla3=  regla4=;
            call symput("fecha_max", cats("&comma_", put(periodo,date9.),"&comma_",'d'));
            put 'Se toma la fecha del mes anterior a la fecha actual ' periodo=;
		end;
	format regla1 regla2 regla3 regla4 periodo date9.;
run;

%macro calculo_score_grl();
*---------------------------------------------------------------*
* Lista los ajustadores con score de las reglas de la familia 6 *
*---------------------------------------------------------------*;
data QCS_PREP_PREV_R6_1_SCORE(keep=  CVE_NOMBRE_AJUSTADOR_GR SCORE  BATCHDATE PERIOD_BATCHDATE  rename=(SCORE=SCORE_6_1) );
  set  &LIBN_SAS..QCS_PREP_PREV_R6_1_SCORE;
  if  missing(CVE_NOMBRE_AJUSTADOR_GR) = 0 and datepart(PERIOD_BATCHDATE)= &fecha_max.;
run;

proc sort data=QCS_PREP_PREV_R6_1_SCORE;
  by CVE_NOMBRE_AJUSTADOR_GR;
quit;

data QCS_PREP_PREV_R6_2_SCORE(keep=  CVE_NOMBRE_AJUSTADOR_GR SCORE  BATCHDATE PERIOD_BATCHDATE  rename=(SCORE=SCORE_6_2) );
  set  &LIBN_SAS..QCS_PREP_PREV_R6_2_SCORE;
  if  missing(CVE_NOMBRE_AJUSTADOR_GR) = 0 and datepart(PERIOD_BATCHDATE)= &fecha_max.;
run;
proc sort data=QCS_PREP_PREV_R6_2_SCORE;
  by CVE_NOMBRE_AJUSTADOR_GR;
quit;

data QCS_PREP_PREV_R6_3_SCORE(keep=  CVE_NOMBRE_AJUSTADOR_GR SCORE  BATCHDATE PERIOD_BATCHDATE  rename=(SCORE=SCORE_6_3) );
  set  &LIBN_SAS..QCS_PREP_PREV_R6_3_SCORE;
  if  missing(CVE_NOMBRE_AJUSTADOR_GR) = 0 and datepart(PERIOD_BATCHDATE)= &fecha_max.;;
run;
proc sort data=QCS_PREP_PREV_R6_3_SCORE;
  by CVE_NOMBRE_AJUSTADOR_GR;
quit;


data QCS_PREP_PREV_R6_4_SCORE(keep=  CVE_NOMBRE_AJUSTADOR_GR SCORE  BATCHDATE PERIOD_BATCHDATE  rename=(SCORE=SCORE_6_4) );
  set  &LIBN_SAS..QCS_PREP_PREV_R6_4_SCORE;
  if  missing(CVE_NOMBRE_AJUSTADOR_GR) = 0 and datepart(PERIOD_BATCHDATE)= &fecha_max.;;
run;

proc sort data=QCS_PREP_PREV_R6_4_SCORE;
  by CVE_NOMBRE_AJUSTADOR_GR;
quit;

DATA WORK.AJUSTADORES_SCORE;
 MERGE WORK.QCS_PREP_PREV_R6_1_SCORE
       WORK.QCS_PREP_PREV_R6_2_SCORE
       WORK.QCS_PREP_PREV_R6_3_SCORE
       WORK.QCS_PREP_PREV_R6_4_SCORE;
 BY CVE_NOMBRE_AJUSTADOR_GR;
RUN;
*------------------------------------------------------------------------------------------*
* Obtiene el porcentaje de score general de las reglas para cada ajustador de la familia 6 *
*------------------------------------------------------------------------------------------*;

DATA WORK.SCORE_GENERAL_F6;
 SET WORK.AJUSTADORES_SCORE;

   IF _N_ EQ 1 THEN DO;
      SET WORK.RULES_RANK_TR (DROP=_NAME_);
   END;

   F6_REGLA_6_1=SCORE_6_1 * REGLA_6_1;
   F6_REGLA_6_2=SCORE_6_2 * REGLA_6_2;
   F6_REGLA_6_3=SCORE_6_3 * REGLA_6_3;
   F6_REGLA_6_4=SCORE_6_4 * REGLA_6_4;

   SCORE_F=SUM(F6_REGLA_6_1, F6_REGLA_6_2, F6_REGLA_6_3, F6_REGLA_6_4 );

   LENGTH SYSUSERID $40. ;

   SYSUSERID="&SYSUSERID.";
RUN;

*---------------------------------------------*
* Obtiene el maximo del score por importancia *
*---------------------------------------------*;
PROC SQL NOPRINT;
    SELECT MAX(SCORE_F) FORMAT=BEST12.
     INTO :MAX_SCORE_F
        FROM WORK.SCORE_GENERAL_F6;
QUIT;

%put  &=MAX_SCORE_F;

RUN;
DATA WORK.SCORE_GENERAL_F6;
 SET WORK.SCORE_GENERAL_F6;
 SCORE=DIVIDE(SCORE_F,&MAX_SCORE_F.);
RUN;
*-------------------------------------------------------------------*
* Asigna el valor cero a los porcentajes que no obtuvieron un valor *
*-------------------------------------------------------------------*;
DATA WORK.SCORE_GENERAL_F6_;
 SET WORK.SCORE_GENERAL_F6;
  DROP _ROW_;
  ARRAY _VARS_(*) _NUMERIC_;
	DO _ROW_=1 TO DIM(_VARS_);
	 IF _VARS_(_ROW_)=. THEN _VARS_(_ROW_)=0;
	END;
RUN;
*--------------------------------------------------------*
* Ordena por el score general las reglas de la familia 6 *
*--------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.QCS_PREP_PREV_F6_SCORE AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
			t1.F6_REGLA_6_1 AS SCORE_6_1,
			t1.F6_REGLA_6_2 AS SCORE_6_2,
			t1.F6_REGLA_6_3 AS SCORE_6_3,
			t1.F6_REGLA_6_4 AS SCORE_6_4,
			t1.SCORE,
			t1.SYSUSERID,
			t1.BATCHDATE,
            t1.PERIOD_BATCHDATE
		FROM
			WORK.SCORE_GENERAL_F6_ t1
	ORDER BY SCORE DESC
    ;
QUIT;
RUN;
*-----------------------------------------------------*
* Genera la tabla del score general para la familia 6 *
*-----------------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_F6_SCORE 
          where trunc(PERIOD_BATCHDATE) =TRUNC(TO_DATE(&fec_fin_ora,'YYYY-MM-DD HH24:MI:SS')) ) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_F6_SCORE
		SELECT 
            CVE_NOMBRE_AJUSTADOR_GR,
			SCORE_6_1,
            SCORE_6_2,
            SCORE_6_3,
            SCORE_6_4,
            SCORE,
            SYSUSERID,
            BATCHDATE,
			PERIOD_BATCHDATE          
		FROM
			WORK.QCS_PREP_PREV_F6_SCORE;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_F6_SCORE 
     where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))

) by oracle;
disconnect from oracle;
QUIT;
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