%include 
	"/qcs/projects/default/qcsrun/sas/macros/prevencion/macros_utility_prev.sas";
%create_log(ruta_log=/qcs/projects/default/qcsrun/logs/prevencion/familia5/, 
	nombre_log=qcs_flow_prev_r5_2);

proc format;
	picture dt_oracle other='%Y-%0m-%0d %0H:%0M:%0S' (datatype=datetime);
run;
/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
/*%obtiene_periodo(fecha_especifica=, intervalo_mes=12, 
	num_meses_hist=12);*/
%include '/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_asigna_periodo_reglas.sas';

%include "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_prod_asegurado_unico.sas";
%include "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_r5_2.sas";
%include "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_score_code_prev_r5_2.sas";
%include "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_cas_code_prev_r5_2.sas";
%close_log();