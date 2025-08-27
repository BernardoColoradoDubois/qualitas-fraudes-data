*Modificación de codigo en regla 5.1, solicitada por Marco García y Daniel Magdaleno el 05/05/2023. 
Se agrega las variables “NOMBRE AGENTE, CONDUCTOR, FECHA REPORTE y REPORTE”. 
Las prep-tables que se modifican son:

•   stg_pago_proveedores
•   stg_pago_proveedores_2 (NUEVO INSUMO)
•   pago_proveedores2
•   ba4_prod2_pag_provee
•   ba6_prod2_pag_provee4
•   TMP_QCS_PREP_PREV_R5_2_ENGINE	

Modificación realizada por Jacqueline Madrazo (JMO) el 29/05/2023.
************************************************************************************************************************************;

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

/*Obtiene parámetros */
/*Lectura de parámetros */
data LISTA_ASEG_excluye;
	set &lib_resu..QCS_PARAM_PREV_R5_2;

	if upcase(kstrip(PARAMETER_NAME))="LISTA_ASEGURADOS" and 
		missing(PARAMETER_VALUE) ne 1 then
			do;
			output LISTA_ASEG_excluye;
		end;
run;

/*Base1: Periodo de analisis*/
proc sql;
	connect to Oracle as 
		connInsumos (path=&path_db. user=&user_db. password=&pwd_user_db.);
	create table work.stg_produccion2 as select * from connection to 
		connInsumos (select trim(poliza) as poliza, trim(endoso) as endoso, trim(inc) 
		as inciso, trim(serie) as serie, trim(nom_aseg) as nom_asegurado, 
		trim(cve_agte) as cve_agente, prima_cade, prima_exded, trunc(fec_emi) as 
		fec_emi , trunc(INI_VIG) as INI_VIG from &schema_db..produccion2);
	disconnect from connInsumos;
quit;

/*Base1: Recuperación de polizas individuales*/
data work.poliza_flotilla;
	set work.stg_produccion2 (keep=INCISO POLIZA);
	length INCISO2 $10.;
	INCISO2=compress(compress(INCISO), "0123456789", 'kis');
	INCISO_num=input(compress(INCISO2, , "kw") , best32.);

	if INCISO_num >  1 then
		do;
			output work.poliza_flotilla;
		end;
run;

proc sql;
	create table work.b1_pol_indv as select b.* from work.stg_produccion2 b where 
		poliza not in (select distinct poliza from work.poliza_flotilla);
quit;

/*Valida datos duplicados de producción2*/
proc sort data=b1_pol_indv;
	by poliza endoso inciso serie fec_emi INI_VIG;
	quit;

data b1_pol_indv2 dup_poliza_serie_fec_emi;
	set b1_pol_indv;
	by poliza endoso inciso serie fec_emi INI_VIG;

	if last.INI_VIG then
		do;
			output b1_pol_indv2;
		end;

	if first.INI_VIG=1 and last.INI_VIG=1 then
		do;
		end;
	else
		do;
			output dup_poliza_serie_fec_emi;
		end;
run;

/*
Valida si una si una póliza_individual-siniestro tiene más de una clave de agente, si fuera el caso aplicar
las sigueinte regla.
regla1: tomar la clave de agente con endoso = 000000
regla2: si no tuviera endoso 000000 tomar la clave de agente de la fecha de emision mas antigua.
*/
data b1_pol_indv2;
	set b1_pol_indv2;
	tmp_endoso=compress(compress(endoso), "0123456789", 'kis');
	tmp_endoso_num=input(compress(tmp_endoso, , "kw") , best32.);

	if kstrip(endoso)='000000' then
		do;
			flg_endoso_0=1;
		end;
	else
		do;
			flg_endoso_0=0;
		end;
run;

proc sort data=b1_pol_indv2 out=std_poliza_cv_agente;
	by poliza tmp_endoso_num;
	quit;

data std_poliza_cv_agente2;
	length CVE_AGENTE_STD $20.;
	set std_poliza_cv_agente;
	by poliza tmp_endoso_num;
	retain CVE_AGENTE_STD;

	if first.poliza and kstrip(endoso)='000000' then
		do;
			CVE_AGENTE_STD=CVE_AGENTE;
		end;
	else
		do;
			CVE_AGENTE_STD=CVE_AGENTE_STD;
		end;
run;

proc sort data=std_poliza_cv_agente2(keep=poliza FEC_EMI CVE_AGENTE 
		CVE_AGENTE_STD where=(CVE_AGENTE_STD='')) out=no_edoso_0;
	by poliza FEC_EMI;
	quit;

proc sort data=no_edoso_0 nodupkey;
	by poliza;
	quit;

proc sql;
	update std_poliza_cv_agente2 as b set CVE_AGENTE_STD=(select CVE_AGENTE from 
		no_edoso_0 as n where b.poliza=n.poliza) where poliza in  (select poliza from 
		no_edoso_0);
quit;

data b1_pol_indv2;
	set std_poliza_cv_agente2;
run;

/*Excluye nombres de asegurados */
/*Valida si existe lista de exclusión  */
%let num_aseg_excluye= %get_num_obs(LISTA_ASEG_excluye);
%put  &=num_aseg_excluye;

%macro excluye_asegurados();
	%if %eval(&num_aseg_excluye  >  0) %then
		%do;
			%put NOTE: excluye asegurados de produccion2;

			/*Excluye lista de asegurados de produccion2 */
			data tmp_b1_pol_indv2;
				set b1_pol_indv2;
			run;

			proc sql;
				create table b1_pol_indv2 as select * from tmp_b1_pol_indv2 where 
					upcase(compress(nom_asegurado, , "kw") ) not in (select 
					upcase(compress(PARAMETER_VALUE, , "kw")) from LISTA_ASEG_excluye);
			quit;

		%end;
	%else
		%do;
			%put NOTE: NO excluye asegurados de produccion2;
		%end;
%mend;

%excluye_asegurados();

/* Polizas con cobertura CADE y Exención de Deducible */
proc sql;
	create table work.base3_cade_exded as select a.poliza from 
		work.stg_produccion2 a where a.prima_cade > 0 or a.prima_exded > 0;
quit;

proc sort data=base3_cade_exded nodupkey;
	by poliza;
	quit;

	/* En pagosproveedores: Base 2: Consolidacion de siniestros de Pagos proveedores */
proc sql;
	connect to Oracle as 
		connInsumos (path=&path_db. user=&user_db. password=&pwd_user_db.);
	create table stg_pago_proveedores as select * from connection to 
		connInsumos (select trim(poliza) as poliza, trim(nro_siniestro) as siniestro, 
		trim(subramo) as subramo, trim(servicio_vehiculo) as servicio_vehiculo , 
		importe_pago, FECHA_PAGO, COBERTURA, trim(AJUSTADOR) as AJUSTADOR, 
		FECHA_OCURRIDO
		from &schema_db..pagosproveedores where trim(poliza) IS NOT 
		NULL and trim(nro_siniestro) IS NOT NULL and trunc(FECHA_OCURRIDO) between 
		TRUNC(TO_DATE(&fec_inicio_ora, 'YYYY-MM-DD HH24:MI:SS')) and 
		TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) );
	disconnect from connInsumos;
quit;

/* Inicio inclusión de modificación - JMO 29/05/2023 */
/* Agrega datos de apertura de reporte a la tabla base de pagos proveedores */
proc sql;
	create table stg_pago_proveedores_2 as select a.*,
	b.CONDUCTOR, b.REPORTE, b.FEC_HORA_REP as FEC_REP,  /*XM SE ELIMINO FORMATO POR ERROR EN LA TABLA RESULTANTE*/
	b.NOM_AGENTE
	from stg_pago_proveedores as a
	left join &lib_insu..APERTURA_REPORTE as b on 
	a.SINIESTRO =(CATS(SUBSTR(b.REPORTE,1,4), b.SINIESTRO)) and 
	a.POLIZA = b.POLIZA; 
quit;
/* Fin inclusión de modificación - JMO 29/05/2023 */

proc sql;
	/* Suma de importe_pago x siniestro. */
	create table pago_x_siniestro as select poliza, siniestro, sum(importe_pago) 
		as sum_imp_pago_x_siniestro from stg_pago_proveedores group by poliza, 
		siniestro;

	/* Cuenta de siniestros x poliza. */
	create table siniestros_x_pol as select poliza, count(distinct siniestro) as 
		siniestros_x_poliza from pago_x_siniestro b group by poliza;
	create table pag_prov_calculado as select b.*, s.siniestros_x_poliza from 
		pago_x_siniestro b inner join siniestros_x_pol s on b.poliza=s.poliza;

	/*Une datos calculados a pagos_proveedores*/
	create table pago_proveedores2 as select s.*, p.sum_imp_pago_x_siniestro, 
		p.siniestros_x_poliza 
		/*from stg_pago_proveedores s left join*/
		from stg_pago_proveedores_2 s  /* Actualización del nuevo insumo - JMO 29/05/2023 */ 
		left join pag_prov_calculado p on 
		s.poliza=p.poliza and s.siniestro=p.siniestro;
quit;

proc sort data=pago_proveedores2;
	by poliza siniestro descending FECHA_PAGO;
	quit;

proc sort data=pago_proveedores2 out=pago_proveedores2_uniq nodupkey;
	by poliza siniestro;
	quit;

proc sql;
	create table ba4_prod2_pag_provee as select b.*, p.siniestro, p.SUBRAMO, 
		p.SERVICIO_VEHICULO, p.cobertura, p.FECHA_PAGO, p.SUM_IMP_PAGO_X_SINIESTRO, 
		p.siniestros_x_poliza, p.ajustador, 
		p.FECHA_OCURRIDO, /* Inclusión del campo FECHA OCURRIDO – JMO 29/05/2023 -XM SE ELIMINO FORMATO POR ERROR EN LA TABLA RESULTANTE*/
		p.CONDUCTOR, /* Inclusión del campo CONDUCTOR – JMO 29/05/2023 */
		p.REPORTE, /* Inclusión del campo REPORTE – JMO 29/05/2023 */
		p.FEC_REP, /* Inclusión del campo FECHA REPORTE – JMO 29/05/2023 */
		p.NOM_AGENTE /* Inclusión del campo NOMBRE AGENTE – JMO 29/05/2023 */
		from b1_pol_indv2 b inner join 
		pago_proveedores2_uniq p on kstrip(b.poliza)=kstrip(p.poliza);

	/*Asigna bandera para poliza  PRIMA_CADE>0 o PRIMA_EXDED>0 */
	create table ba4_prod2_pag_provee2 as select b.*, case when missing(e.poliza) 
		ne 1 then 1 else 0 end as flg_cade_exded from ba4_prod2_pag_provee b left 
		join base3_cade_exded e on b.poliza=e.poliza;
quit;

/*Obtiene campo de tipo_persona de la fuente PROD2_NOM_ASEG_UNICO */
proc sort data=&lib_resu..PROD2_NOM_ASEG_UNICO(keep=NOM_ASEG NOM_ASEG_GR 
		TIPO_PERSONA) out=PROD2_NOM_ASEG_UNICO_ORD;
	by NOM_ASEG_GR TIPO_PERSONA;
	quit;

data PROD2_NOM_ASEG_UNICO_ORD2;
	length TIPO_PERSONA2 $7.;
	set PROD2_NOM_ASEG_UNICO_ORD;
	by NOM_ASEG_GR TIPO_PERSONA;
	retain TIPO_PERSONA2;

	if first.NOM_ASEG_GR then
		do;
			TIPO_PERSONA2=TIPO_PERSONA;
		end;
	else
		do;
			TIPO_PERSONA2=TIPO_PERSONA2;
		end;
run;

proc sort data=PROD2_NOM_ASEG_UNICO_ORD2(keep=NOM_ASEG TIPO_PERSONA2 
		rename=(TIPO_PERSONA2=TIPO_PERSONA)) out=prod2_ASEG_UNICO nodupkey;
	by NOM_ASEG;
	quit;

	/*Asigna tipo de persona */
data ba4_prod2_pag_provee2;
	set ba4_prod2_pag_provee2;
	unique_key2=_N_;
run;

proc sql;
	create table ba4_prod2_pag_provee3 as select b.*, case when 
		missing(u.TIPO_PERSONA) ne 1 then u.TIPO_PERSONA else 'UNKNOWN' end as 
		TIPO_PERSONA from ba4_prod2_pag_provee2 b left join prod2_ASEG_UNICO u on 
		upcase(compress(kstrip(nom_asegurado), , 
		"kw"))=upcase(compress(kstrip(NOM_ASEG), , "kw"));
quit;

proc sort data=ba4_prod2_pag_provee3 nodupkey;
	by unique_key2;
	quit;
  
	/*Base 6: Frecuencia de pagos por póliza (mayor a 3 siniestros).  */
data ba6_prod2_pag_provee3;
	set ba4_prod2_pag_provee3;

	if siniestros_x_poliza > 3;
run;

proc sql;
	create table ba6_prod2_pag_provee4 as select distinct POLIZA , INCISO , 
		NOM_ASEGURADO , CVE_AGENTE_STD as CVE_AGENTE, PRIMA_CADE, PRIMA_EXDED, 
		FLG_CADE_EXDED, SINIESTRO, SUBRAMO, SERVICIO_VEHICULO , COBERTURA, AJUSTADOR, 
		SUM_IMP_PAGO_X_SINIESTRO, SINIESTROS_X_POLIZA, TIPO_PERSONA, flg_endoso_0,
		FECHA_OCURRIDO, /* Inclusión del campo FECHA OCURRIDO – JMO 29/05/2023 */
		CONDUCTOR, /* Inclusión del campo CONDUCTOR – JMO 29/05/2023 */
		REPORTE, /* Inclusión del campo REPORTE – JMO 29/05/2023 */
		FEC_REP, /* Inclusión del campo FECHA REPORTE – JMO 29/05/2023 */
		NOM_AGENTE /* Inclusión del campo NOMBRE AGENTE – JMO 29/05/2023 */
		from ba6_prod2_pag_provee3;
quit;

/*Ordena póliza en base al endos=000000*/
proc sort data=ba6_prod2_pag_provee4;
	by POLIZA INCISO CVE_AGENTE SINIESTRO descending flg_endoso_0;
	quit;

	/*Toma los datos de POLIZA, INCISO y CVE_AGENTE que tienen endoso=00000 más los datos de siniestros */
proc sort data=ba6_prod2_pag_provee4 out=ba6_prod2_pag_provee5 nodupkey;
	by POLIZA INCISO CVE_AGENTE SINIESTRO;
	quit;

data TMP_QCS_PREP_PREV_R5_2_ENGINE
	(keep=/*Inicia datos de produccion2 */
	POLIZA /*ENDOSO*/
	INCISO /*FEC_EMI*/
	NOM_ASEG CVE_AGTE PRIMA_CADE PRIMA_EXDED FLG_CADE_EXDED

	/*Termina datos de produccion2 */
	/*Inicia datos de pagos_proveedores */
	SINIESTRO SUBRAMO SERVICIO_VEHICULO COBERTURA AJUSTADOR
	FECHA_OCURRIDO /* Inclusión del campo FECHA OCURRIDO – JMO 29/05/2023 */
	CONDUCTOR /* Inclusión del campo CONDUCTOR – JMO 29/05/2023 */
	REPORTE /* Inclusión del campo REPORTE – JMO 29/05/2023 */
	FEC_REP /* Inclusión del campo FECHA REPORTE – JMO 29/05/2023 */
	NOM_AGENTE /* Inclusión del campo NOMBRE AGENTE – JMO 29/05/2023 */

	/*Termina datos de pagos_proveedores */
	/*Inicia datos calculados */
	FLG_CADE_EXDED SUM_IMP_PAGO_X_SINIESTRO SINIESTROS_X_POLIZA TIPO_PERSONA 
		BATCHDATE PERIOD_BATCHDATE

		/*Termina datos calculados */);
	set ba6_prod2_pag_provee5 (rename=(NOM_ASEGURADO=NOM_ASEG 
		CVE_AGENTE=CVE_AGTE));
	BATCHDATE=datetime();
	PERIOD_BATCHDATE=DHMS(&fec_fin., 0, 0, 0);
	format PERIOD_BATCHDATE BATCHDATE datetime20.;
run;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");
	execute(delete from &schema_db2..QCS_PREP_PREV_R5_2_ENGINE where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

proc append base=&lib_resu..QCS_PREP_PREV_R5_2_ENGINE 
		data=TMP_QCS_PREP_PREV_R5_2_ENGINE force;
	quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a 12 meses */
	execute(delete from &schema_db2..QCS_PREP_PREV_R5_2_ENGINE where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;

proc datasets lib = WORK nolist nowarn memtype = (data view);
   delete 
ASEG_X_SINIESTRO
LISTA_ASEG_EXCLUYE
MEANS_PAG_SIN_X_ASEGURADO
NOMBRE_ASEGURADO
PERIODOS
SCORE_AEGURADO_RANK3
SCORE_ASEGURADO
SCORE_ASEGURADO2
SCORE_ASEGURADO_RANK
SCORE_ASEGURADO_RANK2
STG_ENGINE_SCORE
STG_PRODUCCION2;
quit;

Libname &lib_insu. clear;
Libname &lib_resu. clear;
cas session_load terminate;
