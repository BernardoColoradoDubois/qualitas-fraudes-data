%include '/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_librerias_rutas2.sas';
%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macros_utility_prev.sas';

	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

/*
Genera score de deducible pérdida total para los siguiente grupos:
1) Daño material con perdida total   ( valuacion. NUM_PT=1 )
2) Daño material con robo total
*/
/*Genera tabla base para el score */
data DANOS_MATERIALES (drop=PERIOD_BATCHDATE);
	set &LIB_RESU..QCS_PREP_PREV_R3_1_ENGINE_PT (keep=DEDUCIBLE_NETEADO 
		DEDUCIBLE_FAL OFICINA_ATENCION FLG_ALERT ASEGURADO PERIOD_BATCHDATE);

	if FLG_ALERT=1 and missing(OFICINA_ATENCION) ne 1 and 
		datepart(PERIOD_BATCHDATE)=&fec_fin.;
run;

/*Frecuencia por oficina de atención  y Suma de DEDUCIBLE_FAL por oficina de atención*/
proc sql;
	create table freq_oficina as select OFICINA_ATENCION, count(*) as DEDUC_FAL_N, 
		sum(DEDUCIBLE_FAL) as DEDUC_FAL_SUM, COUNT(ASEGURADO) AS cnt_asegurado from 
		DANOS_MATERIALES group by OFICINA_ATENCION;
quit;

PROC RANK DATA=WORK.freq_oficina OUT=DANOS_MAT DESCENDING /*TIES=low*/;
	VAR DEDUC_FAL_N DEDUC_FAL_SUM;
	RANKS RNK1_DEDUC_FAL_N RNK2_DEDUC_FAL_SUM;
RUN;

data DANOS_MAT;
	set DANOS_MAT;
	RANK_SUM=sum((0.6 * RNK1_DEDUC_FAL_N) , (0.4*RNK2_DEDUC_FAL_SUM) );
run;

proc sql;
	CREATE TABLE QCS_PREP_PREV_R3_1_SCORE_PT AS SELECT t1.OFICINA_ATENCION as 
		OFC_ATENCION , t1.DEDUC_FAL_N, t1.DEDUC_FAL_SUM, t1.RNK1_DEDUC_FAL_N, 
		t1.RNK2_DEDUC_FAL_SUM, t1.RANK_SUM, MIN(t1.RANK_SUM)/t1.RANK_SUM AS SCORE_DM, 
		DATETIME() AS BATCHDATE FORMAT=DATETIME20., "&SYSUSERID." AS SYSUSERID, 
		DHMS(&fec_fin., 0, 0, 0) as PERIOD_BATCHDATE FORMAT=DATETIME20.
		FROM WORK.DANOS_MAT t1 order by DEDUC_FAL_N desc , SCORE_DM;
quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2
		path="&path_db2");
	execute(delete from  &schema_db2..QCS_PREP_PREV_R3_1_SCORE_PT where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

proc append base=&LIB_RESU..QCS_PREP_PREV_R3_1_SCORE_PT 
		data=QCS_PREP_PREV_R3_1_SCORE_PT force;
	quit;

	/*Solo se dejan doce meses en la historia*/
proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a 12 meses */
	execute(delete from &schema_db2..QCS_PREP_PREV_R3_1_SCORE_PT where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;

cas session_load terminate;