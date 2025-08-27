%include "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_librerias_rutas2.sas";
%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macros_utility_prev.sas';

/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

data stg_engine_score;
	set &lib_resu..QCS_PREP_PREV_R5_2_ENGINE (keep=NOM_ASEG TIPO_PERSONA poliza 
		SINIESTRO SUM_IMP_PAGO_X_SINIESTRO SINIESTROS_X_POLIZA PERIOD_BATCHDATE 
		FLG_CADE_EXDED);

 /*Para el cálculo del score se toma las pólizas que tengan mas de tres siniestros O las polizas 
   con prima PRIMA_CADE>0 o PRIMA_EXDED>0 */

	if missing(NOM_ASEG) ne 1 and datepart(PERIOD_BATCHDATE)=&fec_fin.;
run;

proc sort data=stg_engine_score out=aseg_x_siniestro nodupkey;
	by NOM_ASEG poliza siniestro;
	quit;

PROC MEANS DATA=aseg_x_siniestro N MEAN MEDIAN NOPRINT NWAY;
	VAR SUM_IMP_PAGO_X_SINIESTRO;
	CLASS NOM_ASEG POLIZA;
	OUTPUT OUT=means_pag_sin_x_asegurado (DROP=_TYPE_ _FREQ_) N=MEAN=MEDIAN= / 
		AUTONAME;
RUN;

data score_asegurado(keep=NOM_ASEG POLIZA frecuencia_siniestro 
		mediana_importe_pago uniq_key3);
	set means_pag_sin_x_asegurado (rename=(SUM_IMP_PAGO_X_SINIESTRO_N=frecuencia_siniestro 
		SUM_IMP_PAGO_X_SINIESTR_Median=mediana_importe_pago));
	uniq_key3=_N_;
run;

/*Asocia el campo tipo de persona por el nom_aseg */
proc sort data=stg_engine_score(keep=NOM_ASEG TIPO_PERSONA) 
		out=nombre_asegurado ;
	by NOM_ASEG TIPO_PERSONA;
	quit;

proc sort data=nombre_asegurado nodupkey;
  by NOM_ASEG ;
quit;

proc sql;
	create table score_asegurado2 as select s.*, a.TIPO_PERSONA from 
		score_asegurado s left join nombre_asegurado a on compress(s.NOM_ASEG, , 
		"kw")=compress(a.NOM_ASEG, , "kw");
quit;

proc sort data=score_asegurado2 nodupkey;
	by uniq_key3;
	quit;

PROC RANK DATA=score_asegurado2 OUT=score_asegurado_rank DESCENDING;
	VAR frecuencia_siniestro mediana_importe_pago;
	RANKS RNK1_frecuencia_siniestro RNK2_mediana_importe_pago;
RUN;

data score_asegurado_rank2;
	set score_asegurado_rank;
	rank_sum=sum(RNK1_frecuencia_siniestro, RNK2_mediana_importe_pago);
run;

proc sql;
	create table score_aegurado_rank3 as select tipo_persona , NOM_ASEG as 
		NOMBRE_ASEGURADO, poliza, frecuencia_siniestro, mediana_importe_pago, 
		RNK1_frecuencia_siniestro, RNK2_mediana_importe_pago, rank_sum, min(rank_sum) 
		/ rank_sum as score_f, datetime() as BATCHDATE format=DATETIME20., 
		"&SYSUSERID." AS SYSUSERID length=40, DHMS(&fec_fin., 0, 0, 0) as PERIOD_BATCHDATE 
		format=DATETIME20.
		from score_asegurado_rank2 order by calculated score_f desc;
quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");
	execute(delete from &schema_db2..QCS_PREP_PREV_R5_2_SCORE where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

data score_aegurado_rank3;
	 set score_aegurado_rank3;
    if missing(NOMBRE_ASEGURADO) ne 1;
run;

proc append base=&lib_resu..QCS_PREP_PREV_R5_2_SCORE data=score_aegurado_rank3 
		force;
	quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a doce meses */
	execute(delete from &schema_db2..QCS_PREP_PREV_R5_2_SCORE where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;

Libname &lib_insu. clear;
Libname &lib_resu. clear;
cas session_load terminate;