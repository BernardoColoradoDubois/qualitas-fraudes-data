/* -------------------------------------------------------
	    QUALITAS COMPAÑIA DE SEGUROS - AREA DE FRAUDES
            AUTOR: SAS Institute Mexico (smxeba)
           		  FECHA: Diciembre 3, 2021
           FLUJO DE PROGRAMAS SAS DE LA REGLA 1.4
   ------------------------------------------------------ */
/*refiere al numero de regla*/
%let seqn_rule=r1_4;
/*refiere al nombre del codigo que invoca los programas de la regla*/
%let name_prog=qcs_flow_prev_&seqn_rule;
/*refiere a la ruta del archivo log del codigo que invoca los programas de la regla*/
%let path_glog=/qcs/projects/default/qcsrun/logs/prevencion/familia1/;
/*obtiene la fecha y hora de ejecucion del archivo log del codigo que invoca los programas de la regla*/
%let flog_dtime=%sysfunc(cats(%sysfunc(today(),date.),_,
		  		%scan(%sysfunc(time(),tod.), 1, :),_,
                %scan(%sysfunc(time(),tod.), 2, :),_,
		        %scan(%sysfunc(time(),tod.), 3, :)));	
proc printto log="&path_glog.&name_prog._&flog_dtime..log"; run;
/*obtiene fecha y hora de inicio de la ejecucion*/
%let _timer_start = %sysfunc(datetime());

%include '/qcs/projects/default/qcsrun/sas/macros/prevencion/macros_utility_prev.sas';
proc format;
	picture dt_oracle other='%Y-%0m-%0d %0H:%0M:%0S' (datatype=datetime);
run;

/*Carga fecha especifica */
/*%obtiene_periodo(fecha_especifica=31/07/2022, intervalo_mes=19, num_meses_hist=12); */ 
/*%obtiene_periodo(fecha_especifica=, intervalo_mes=12, num_meses_hist=12); */
%include '/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_asigna_periodo_reglas.sas';

*-----------------------------------------------------------------------------------
* Invoca programa de actualizacion de las PREP TABLES (MOTORA, DETALLE y RESULTADO) 
*-----------------------------------------------------------------------------------;
filename prepstab "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_&seqn_rule..sas";
%include prepstab;
*--------------------------------------------------
* Invoca programa de actualizacion de SCORE TABLES
*--------------------------------------------------;
filename scoretab "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_score_code_prev_&seqn_rule..sas";
%include scoretab;
*-----------------------------------------------------------
* Invoca programa de actualizacion de las CAS MEMORY TABLES 
*-----------------------------------------------------------;
filename castable "/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_cas_code_prev_&seqn_rule..sas";
%include castable;
/*obtiene la duracion en tiempo de la ejecucion*/
data _null_;
 dur = datetime() - &_timer_start;
 put 80*'-' / "NOTE: Execution time of &name_prog..sas:" dur time13.2 / 80*'-';
run;
proc printto;
run;