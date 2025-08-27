*Modificación de código en regla 5.1, solicitada por Marco García y Daniel Magdaleno el 05/05/2023. 
Se cambia la ponderación para el cálculo del score. La prep-table que se modifica es:

•	RANKS

Modificación realizada por Jacqueline Madrazo (JMO) el 22/05/2023.
*************************************************************************************************************************************;

%include '/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_librerias_rutas2.sas';
%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macros_utility_prev.sas';

/*Obtiene parámetros */
/*Lectura de parámetros */
data LISTA_ASEGURADOS_excluye;
	set &lib_resu..QCS_PARAM_PREV_R5_1;

	if upcase(kstrip(PARAMETER_NAME))="VAR_DIAS_INI_VIG" then
		do;
			call symput("DIAS_INI_VIG", PARAMETER_VALUE);
		end;
	else if upcase(kstrip(PARAMETER_NAME))="VAR_DIAS_MONTADO" and 
		missing(PARAMETER_VALUE) ne 1 then
			do;
			call symput("DIAS_MONTADO", PARAMETER_VALUE);
		end;
	else if upcase(kstrip(PARAMETER_NAME))="LISTA_ASEGURADOS" and 
		missing(PARAMETER_VALUE) ne 1 then
			do;
			output LISTA_ASEGURADOS_excluye;
		end;
run;

%put &=DIAS_INI_VIG;
%put &=DIAS_MONTADO;

/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

/*Calculo del score para la regla 5_1*/
data stg_aseg_alertados;
	set  &lib_resu..QSC_PREP_PREV_R5_1_DETAIL (keep=NOMBRE_ASEGURADO_GR 
		SUM_IMPORTE_PAGO FLG_MONTADA FLG_SINIESTRADO TIPO_PERSONA PAGOS_X_ASEGURADO 
		PERIOD_BATCHDATE);

	if FLG_MONTADA=1 and FLG_SINIESTRADO=1 and missing(NOMBRE_ASEGURADO_GR) ne 1 
		and datepart(PERIOD_BATCHDATE)=&fec_fin.;
run;

/*Obtiene el monto de pago por asegurado*/
proc sql;
	create table monto_x_asegurado as select NOMBRE_ASEGURADO_GR, 
		sum(SUM_IMPORTE_PAGO) as sum_monto_pago_x_aseg from stg_aseg_alertados group 
		by NOMBRE_ASEGURADO_GR;
quit;

/*Obtiene el número de pagos por asegurado de la tabla  QSC_PREP_PREV_R5_1_DETAIL,  donde previamente fue calculado*/
proc sort data=stg_aseg_alertados (keep=PAGOS_X_ASEGURADO TIPO_PERSONA 
		NOMBRE_ASEGURADO_GR PERIOD_BATCHDATE) out=pagos_x_asegurado ;
	by NOMBRE_ASEGURADO_GR TIPO_PERSONA;
	quit;

proc sort data=pagos_x_asegurado nodupkey;
	by NOMBRE_ASEGURADO_GR ;
	quit;

proc sql;
	create table aseg_sum_monto_freq as select b.NOMBRE_ASEGURADO_GR, 
		b.sum_monto_pago_x_aseg as sum_pag_sum, f.PAGOS_X_ASEGURADO as sum_pag_n, 
		f.TIPO_PERSONA, f.PERIOD_BATCHDATE from monto_x_asegurado b inner join 
		pagos_x_asegurado f on b.NOMBRE_ASEGURADO_GR=f.NOMBRE_ASEGURADO_GR;
quit;

PROC RANK DATA=WORK.aseg_sum_monto_freq OUT=WORK.RANKS TIES=MEAN DESCENDING;
	VAR SUM_PAG_N SUM_PAG_SUM;
	RANKS RANK1_SUM_PAG_N RANK2_SUM_PAG_SUM;
RUN;

data RANKS;
	set RANKS;
/*	rank_sum=sum((0.6 * RANK1_SUM_PAG_N), (0.4 * RANK2_SUM_PAG_SUM));*/
	rank_sum=sum((0.8 * RANK1_SUM_PAG_N), (0.2 * RANK2_SUM_PAG_SUM)); /* Cambio en la ponderación – JMO 22/05/2023 */
run;

proc sql;
	create table ranks_score as select b.NOMBRE_ASEGURADO_GR, b.TIPO_PERSONA, 
		b.sum_pag_n, b.sum_pag_sum, b.RANK1_SUM_PAG_N, b.RANK2_SUM_PAG_SUM, 
		b.rank_sum
		, min(b.rank_sum) as min_rank_sum, min(b.rank_sum) / b.rank_sum as score_f, 
		PERIOD_BATCHDATE, datetime() as BATCHDATE, "&SYSUSERID." AS SYSUSERID from 
		RANKS b order by SUM_PAG_N, calculated score_f;
quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2
		path="&path_db2");
	execute(delete from &schema_db2..QCS_PREP_PREV_R5_1_SCORE where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

data ranks_score;
	 set ranks_score;
	 if missing(PERIOD_BATCHDATE) = 0;
run;

proc append base=&lib_resu..QCS_PREP_PREV_R5_1_SCORE data=ranks_score force;
	quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a doce meses */
	execute(delete from &schema_db2..QCS_PREP_PREV_R5_1_SCORE where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;


cas session_load terminate;