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
Genera score para el grupo de Deducible con no pérdida Total
1) Daño material con no pérdida Total ( valuacion. NUM_PT=0 )
*/
/*Genera tabla base para el score */
data DANOS_MATERIALES(rename=(OFICINA_ATENCION_2=OFICINA_ATENCION) 
		drop=PERIOD_BATCHDATE);
	set &LIB_RESU..QCS_PREP_PREV_R3_1_ENGINE_DM (keep=dif_de_ingreso 
		OFICINA_ATENCION_2 FLG_ALERT PERIOD_BATCHDATE);

	if FLG_ALERT=1 and missing(OFICINA_ATENCION_2) ne 1 and 
		datepart(PERIOD_BATCHDATE)=&fec_fin.;
run;

/*Frecuencia por oficina de atención  y Suma de DEDUCIBLE_FAL por oficina de atención*/
proc sql;
	create table freq_oficina as select OFICINA_ATENCION, count(*) as 
		DIF_DE_INGRESO_N, sum(dif_de_ingreso) as DIF_DE_INGRESO_SUM from 
		DANOS_MATERIALES group by OFICINA_ATENCION;
quit;

PROC RANK DATA=WORK.freq_oficina OUT=DANOS_MAT DESCENDING;
	VAR DIF_DE_INGRESO_N DIF_DE_INGRESO_SUM;
	RANKS RNK1_DIF_DE_INGRESO_N RNK2_DIF_DE_INGRESO_SUM;
RUN;

data DANOS_MAT;
	set DANOS_MAT;
	RANK_SUM=sum((0.8 * RNK1_DIF_DE_INGRESO_N) , (0.2* RNK2_DIF_DE_INGRESO_SUM) );
run;

proc sql;
	CREATE TABLE QCS_PREP_PREV_R3_1_SCORE_DM AS SELECT t1.OFICINA_ATENCION as 
		OFC_ATENCION , t1.DIF_DE_INGRESO_N, t1.DIF_DE_INGRESO_SUM, 
		t1.RNK1_DIF_DE_INGRESO_N, t1.RNK2_DIF_DE_INGRESO_SUM, t1.RANK_SUM, 
		MIN(t1.RANK_SUM)/t1.RANK_SUM AS SCORE_NPT, DATETIME() AS BATCHDATE 
		FORMAT=DATETIME20., "&SYSUSERID." AS SYSUSERID, DHMS(&fec_fin., 0, 0, 0) as 
		PERIOD_BATCHDATE FORMAT=DATETIME20. FROM WORK.DANOS_MAT t1 order by 
		t1.DIF_DE_INGRESO_N desc , calculated SCORE_NPT;
quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2
		path="&path_db2");
	execute(delete from  &schema_db2..QCS_PREP_PREV_R3_1_SCORE_DM where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

proc append base=&LIB_RESU..QCS_PREP_PREV_R3_1_SCORE_DM 
		data=QCS_PREP_PREV_R3_1_SCORE_DM force;
	quit;


/*Solo se dejan doce meses en la historia*/
proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a 12 meses */
	execute(delete from &schema_db2..QCS_PREP_PREV_R3_1_SCORE_DM where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;

	cas session_load terminate;