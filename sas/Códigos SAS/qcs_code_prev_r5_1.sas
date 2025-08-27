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

/*Lectura de toda la tabla de produccion2 para evitar falsos positivos en la clasificación 
de una póliza (flotilla, individual) */
proc sql;
	create table stg_produccion2 as select kstrip(POLIZA) as POLIZA, 
		kstrip(ENDOSO) as ENDOSO, kstrip(INC) as INCISO, UPCASE(kstrip(SERIE)) as 
		SERIE , INI_VIG, FIN_VIG, FEC_EMI, KSTRIP(COD_ASEG) as COD_ASEG, 
		UPCASE(KSTRIP(NOM_ASEG)) as NOM_ASEG, KSTRIP(CVE_AGTE) as CVE_AGTE, 
		KSTRIP(NOM_AGTE) as NOM_AGTE  , KSTRIP(NO_OFI) as NO_OFI, FEC_PAGO 
		from  &lib_insu..produccion2;
quit;

/*Excluye pólizas que correspondan a una flotilla */
data poliza_flotilla;
	set stg_produccion2 (keep=INCISO POLIZA);
	length INCISO2 $10.;
	INCISO2=compress(compress(INCISO), "0123456789", 'kis');
	INCISO_num=input(compress(INCISO2, , "kw") , best32.);

	if INCISO_num >  1 then
		do;
			output poliza_flotilla;
		end;
run;

proc sort data=poliza_flotilla(keep=poliza) nodupkey;
	by poliza;
	quit;

proc sql;
	create table stg_produccion2_1 as select b.* from stg_produccion2 b where 
		poliza not in (select poliza from poliza_flotilla) and (datepart(FEC_PAGO) 
		between  &fec_inicio and &fec_fin) and missing(b.SERIE) ne 1 and 
		missing(b.poliza) ne 1 and ENDOSO eq '000000';
quit;

/*Excluye beneficiarios*/
/*Valida si existe lista de exclusión  */
%let num_aseg_excluye= %get_num_obs(LISTA_ASEGURADOS_excluye);
%put  &=num_aseg_excluye;

%macro excluye_asegurado();
	%if %eval(&num_aseg_excluye  >  0) %then
		%do;
			%put NOTE: excluye asegurados de produccion2;

			/*Excluye lista de asegurados de produccion2*/
			data tmp_stg_produccion2_1;
				set stg_produccion2_1;
			run;

			proc sql;
				create table stg_produccion2_1 as select * from tmp_stg_produccion2_1 where 
					upcase(compress(NOM_ASEG, , "kw") ) not in (select 
					upcase(compress(PARAMETER_VALUE, , "kw")) from LISTA_ASEGURADOS_excluye);
			quit;

		%end;
	%else
		%do;
			%put NOTE: NO excluye asegurados de produccion2;
		%end;
%mend;

%excluye_asegurado();

data stg_produccion2_1;
	set stg_produccion2_1;
	key_uniq=_N_;
run;

proc casutil outcaslib="CasUser";
	load data=stg_produccion2_1 CASOUT="stg_produccion2_1" replace;
	quit;

proc casutil incaslib="QCS_Resultados_Planeacion" outcaslib="CasUser";
	load casdata="PROD2_NOM_ASEG_UNICO_R5_1" CASOUT="PROD2_NOM_ASEG_UNICO" replace 
		varlist=((name="NOM_ASEG"), (name="NOM_ASEG_GR"), (name="TIPO_PERSONA"));
	load casdata="PROD2_SERIE_UNICO" CASOUT="PROD2_SERIE_UNICO" replace 
		varlist=((name="SERIE"), (name="SERIE_STND_GR") );
	quit;

data lcasres.stg_produccion2_1;
	set lcasres.stg_produccion2_1 (rename=(NOM_ASEG=tmp_NOM_ASEG));
	attrib COMP_NOM_ASEG length=$300;
	attrib NOM_ASEG length=$300;
	attrib COMP_SERIE length=$250;
	NOM_ASEG=compress(tmp_NOM_ASEG, , "kw");
	COMP_NOM_ASEG=NOM_ASEG;
	COMP_SERIE=dqstandardize (SERIE, "Non-Alphanumeric Removal", "ESMEX");
run;

data lcasres.PROD2_NOM_ASEG_UNICO (drop=TMP_NOM_ASEG_GR);
	set lcasres.PROD2_NOM_ASEG_UNICO (rename=(NOM_ASEG_GR=TMP_NOM_ASEG_GR));
	attrib COMP_NOM_ASEG NOM_ASEG_GR length=$300;
	COMP_NOM_ASEG=NOM_ASEG;
	NOM_ASEG_GR=TMP_NOM_ASEG_GR;

	if missing(COMP_NOM_ASEG) ne 1;
run;

data lcasres.PROD2_SERIE_UNICO (drop=TMP_SERIE_STND_GR);
	set lcasres.PROD2_SERIE_UNICO (rename=(SERIE_STND_GR=TMP_SERIE_STND_GR));
	attrib COMP_SERIE SERIE_STND_GR length=$250;
	COMP_SERIE=dqstandardize (SERIE, "Non-Alphanumeric Removal", "ESMEX");
	SERIE_STND_GR=TMP_SERIE_STND_GR;

	if missing(SERIE_STND_GR) ne 1;
run;

/*Asocia datos de golden record a base principal*/
proc fedsql sessref=session_load;
	create table stg_produccion2_2 {OPTION replace=true replication=0} as select 
		b.*, a.NOM_ASEG_GR as tmp_NOM_ASEG_GR, case when a.NOM_ASEG_GR is not missing 
		then a.NOM_ASEG_GR else b.NOM_ASEG end as NOM_ASEG_GR, a.TIPO_PERSONA from 
		stg_produccion2_1 b left join PROD2_NOM_ASEG_UNICO a on 
		UPCASE(trim(b.COMP_NOM_ASEG))=UPCASE(trim(a.COMP_NOM_ASEG));
	quit;

proc fedsql sessref=session_load;
	create table stg_produccion2_3 {OPTION replace=true replication=0} as select 
		b.*, s.COMP_SERIE as tmp_COMP_SERIE, case when s.SERIE_STND_GR is not missing 
		then s.SERIE_STND_GR else b.SERIE end as SERIE_STND_GR from stg_produccion2_2 
		b left join PROD2_SERIE_UNICO s on 
		upcase(trim(b.COMP_SERIE))=upcase(trim(s.COMP_SERIE));
	quit;

data lcasres.stg_produccion2_3;
	set lcasres.stg_produccion2_3 (drop=tmp_COMP_SERIE tmp_NOM_ASEG_GR 
		COMP_NOM_ASEG COMP_SERIE);
	by key_uniq;

	if first.key_uniq;
run;

/***********************Obtiene polizas montadas para un numero de serie ************************/
/*Obtiene fechas previas de inicio de vigencia y fin de vigencia.*/
data lcasres.stg_produccion2_4;
	set lcasres.stg_produccion2_3;
	attrib PREV_POLIZA length=$10;
	by SERIE_STND_GR INI_VIG FEC_EMI key_uniq;
	PREV_INI_VIG=lag1(INI_VIG);
	PREV_FIN_VIG=lag1(FIN_VIG);
	PREV_PAR_key_uniq=lag1(key_uniq);
	PREV_POLIZA=lag1(POLIZA);

	if first.SERIE_STND_GR=1 then
		do;
			PREV_INI_VIG=.;
			PREV_FIN_VIG=.;
			PREV_PAR_key_uniq=.;
			PREV_POLIZA=.;
		end;
	format PREV_INI_VIG PREV_FIN_VIG DATETIME20.;
run;

data lcasres.stg_produccion2_5;
	set lcasres.stg_produccion2_4;
	dif_dias_ini_vig2=intck('dtday', PREV_INI_VIG, INI_VIG);

	/*Si INI_VIG > PREV_FIN_VIG   el valor de diferencias de días sera negativo */
	dias_mont2=intck('dtday', INI_VIG, PREV_FIN_VIG);

	if POLIZA=PREV_POLIZA then
		do;

			/*
			Flag para excluir la póliza montada con el mismo número de poliza
			Ej. póliza registro actual=54 y póliza registro anterior = 54,la póliza anterior se elimina,
			por otro lado si la poliza actual tiene una póliza siguiente diferente y montada se mantiene
			*/
			flg_igual_poliza2=1;
		end;
	else
		do;
			flg_igual_poliza2=0;
		end;

	if dias_mont2  &DIAS_MONTADO.  and dif_dias_ini_vig2  &DIAS_INI_VIG.  and 
		flg_igual_poliza2 ne 1 then
			do;
			flg_montada2=1;
		end;
	else
		do;
			flg_montada2=0;
		end;
run;

/*Asocia el número de dí­as al primer par de polizas comparada*/
proc fedsql sessref=session_load;
	create table stg_produccion2_6 {options replace=true replication=0} as select 
		b.*, b2.dif_dias_ini_vig2 as dif_dias_ini_vig1, b2.dias_mont2 as dias_mont1 , 
		b2.flg_montada2 as flg_montada1, b2.flg_igual_poliza2 as flg_igual_poliza1 
		from stg_produccion2_5 b left join  (select PREV_PAR_key_uniq , 
		dif_dias_ini_vig2, dias_mont2 , flg_montada2, flg_igual_poliza2 from 
		stg_produccion2_5) b2 on b.key_uniq=b2.PREV_PAR_key_uniq;
	quit;

data lcasres.stg_produccion2_6_

	/* ( drop=flg_montada2 flg_montada1
	dif_dias_ini_vig2 dias_mont2) */
	lcasres.stg_produccion2_6_no_mon

	/* ( drop=flg_montada2 flg_montada1
	dif_dias_ini_vig2 dias_mont2) */;
	set lcasres.stg_produccion2_6;

	if flg_montada2=1 or flg_montada1=1 then
		do;
			FLG_MONTADA=1;
			output lcasres.stg_produccion2_6_;
		end;
	else
		do;
			FLG_MONTADA=0;
			output lcasres.stg_produccion2_6_no_mon;
		end;
		
run;

/*
Si un número de serie tiene polizas montadas, se valida si el nombre del asegurado
es el mismo en cada poliza.
si es el mismo se asigna un valor =0
si no el mismo se asigna un valor =2
*/
proc fedsql sessref=session_load;
	create table serie_num_aseg_dist {options replace=true replication=0} as 
		select SERIE_STND_GR, count(distinct NOM_ASEG_GR) as cnt_aseg_dist from 
		stg_produccion2_6_ group by SERIE_STND_GR;
	quit;

proc fedsql sessref=session_load;
	create table stg_produccion2_7 {options replace=true replication=0} as select 
		b.*, a.cnt_aseg_dist, case when a.cnt_aseg_dist=1 then 0 when 
		a.cnt_aseg_dist > 1 then 2 else 0 end as FLG_MISMO_ASEG from 
		stg_produccion2_6_ b left join serie_num_aseg_dist a on 
		b.SERIE_STND_GR=a.SERIE_STND_GR;
	quit;

	/*	Obtiene datos de pagos proveedores */
proc sql;
	create table stg_pagos_proveedor as select kstrip(POLIZA) as POLIZA, 
		kstrip(NRO_SINIESTRO) as SINIESTRO, CODIGO_DE_PAGO, CVE_DOCTO, IMPORTE_PAGO, 
		FECHA_PAGO, INCISO from   &lib_insu..PAGOSPROVEEDORES where missing(poliza) 
		ne 1 and missing(NRO_SINIESTRO) ne 1 order by poliza, siniestro;
quit;

/*Obtiene datos de stg_pagos_proveedor */
proc sql;
	/* Acumula IMPORTE_PAGO por poliza y siniestro */
	create table agrupa_pol_sin as select POLIZA, SINIESTRO, sum(IMPORTE_PAGO) as 
		SUM_IMPORTE_PAGO from stg_pagos_proveedor group by POLIZA, SINIESTRO;

	/*Agrega datos de importe acumulado a la tabla base de pagos proveedores */
	create table pagos_proveedor2 as select b.*, s.SUM_IMPORTE_PAGO from 
		stg_pagos_proveedor b left join agrupa_pol_sin s on b.poliza=s.poliza and 
		b.siniestro=s.siniestro;
quit;

proc sort data=pagos_proveedor2;
	by poliza siniestro descending FECHA_PAGO;
	quit;

data pagos_proveedor3;
	set pagos_proveedor2;
	by poliza siniestro descending FECHA_PAGO;

	if first.siniestro then
		do;
			output pagos_proveedor3;
		end;
run;

proc casutil OutCasLib='CasUser';
	load data=work.pagos_proveedor3 casout="pagos_proveedor3" replace;
	quit;

	/*Union de datos de pagos proveedores a tabla base de solo polizas montadas*/
proc fedsql sessref=session_load;
	create table stg_produccion2_8 {options replace=true replication=0} as select 
		b.*, p.SINIESTRO, case when  (p.SINIESTRO is not missing) then 1 else 0 end 
		as FLG_SINIESTRADO, p.SUM_IMPORTE_PAGO, p.inciso as inciso_pago_prov, 
		p.CODIGO_DE_PAGO, p.CVE_DOCTO from stg_produccion2_7 b left join 
		pagos_proveedor3 p on b.poliza=p.poliza;
	quit;

	/*
	Conteo de siniestros por Asegurado
	La agrupación de Asegurado, poliza, siniestro unico es igual a un pago
	*/
proc fedsql sessref=session_load;
	create table pagos_x_ASEG_POLIZA {options replace=true replication=0} as 
		select NOM_ASEG_GR, poliza, count(distinct siniestro) as 
		NUM_PAGOS_X_ASEG_POLIZA from stg_produccion2_8 b where FLG_SINIESTRADO=1 
		group by NOM_ASEG_GR, poliza;
	quit;

proc fedsql sessref=session_load;
	create table pagos_x_asegurado {options replace=true replication=0} as select 
		NOM_ASEG_GR, sum(NUM_PAGOS_X_ASEG_POLIZA) as PAGOS_X_ASEGURADO from 
		pagos_x_ASEG_POLIZA group by NOM_ASEG_GR;
	quit;

proc fedsql sessref=session_load;
	create table stg_produccion2_9 {options replace=true replication=0} as select 
		b.*, s.PAGOS_X_ASEGURADO from stg_produccion2_8 b left join pagos_x_asegurado 
		s on b.NOM_ASEG_GR=s.NOM_ASEG_GR;
	quit;

data lcasres.TMP_PREP_PREV_R5_1_ENGINE (drop=SERIE_STND_GR NOM_ASEG_GR 
		FLG_MISMO_ASEG);
	set lcasres.stg_produccion2_9(keep=/*Inicia datos de produccion2 */
	SERIE_STND_GR SERIE POLIZA ENDOSO INCISO NOM_ASEG NOM_ASEG_GR INI_VIG FIN_VIG 
		FEC_EMI NOM_AGTE NO_OFI
		/*Cierra datos de produccion2 */
		/*Inicia datos pagosproveedores*/
		SINIESTRO CODIGO_DE_PAGO CVE_DOCTO
		/*Termina datos pagosproveedores*/
		/*Datos calculados */
		dif_dias_ini_vig1 dias_mont1 SUM_IMPORTE_PAGO PAGOS_X_ASEGURADO FLG_MONTADA 
		FLG_MISMO_ASEG FLG_SINIESTRADO TIPO_PERSONA
		/*Cierra datos calculados */);
	attrib SERIE_GR length=$30;
	attrib NOMBRE_ASEGURADO_GR length=$100;
	SERIE_GR=SERIE_STND_GR;
	NOMBRE_ASEGURADO_GR=kstrip(NOM_ASEG_GR);
	FLG_MISMO_ASEG2=FLG_MISMO_ASEG;
	PERIOD_BATCHDATE=DHMS(&fec_fin., 0, 0, 0);
	format PERIOD_BATCHDATE datetime20.;
run;
  
data lcasres.TMP_PREP_PREV_R5_1_DETAIL(drop=SERIE_STND_GR NOM_ASEG_GR 
		FLG_MISMO_ASEG);
	set lcasres.stg_produccion2_9(keep=/*Inicia datos de produccion2 */
	SERIE_STND_GR SERIE POLIZA ENDOSO INCISO NOM_ASEG NOM_ASEG_GR INI_VIG FIN_VIG 
		FEC_EMI NOM_AGTE NO_OFI
		/*Cierra datos de produccion2 */
		/*Inicia datos pagosproveedores*/
		SINIESTRO CODIGO_DE_PAGO CVE_DOCTO
		/*Termina datos pagosproveedores*/
		/*Datos calculados */
		dif_dias_ini_vig1 dias_mont1 SUM_IMPORTE_PAGO PAGOS_X_ASEGURADO FLG_MONTADA 
		FLG_MISMO_ASEG FLG_SINIESTRADO TIPO_PERSONA
		/*Cierra datos calculados */);
	attrib SERIE_GR length=$30;
	attrib NOMBRE_ASEGURADO_GR length=$100;
	SERIE_GR=SERIE_STND_GR;
	NOMBRE_ASEGURADO_GR=NOM_ASEG_GR;
	FLG_MISMO_ASEG2=FLG_MISMO_ASEG;
	PERIOD_BATCHDATE=DHMS(&fec_fin., 0, 0, 0);
	format PERIOD_BATCHDATE datetime20.;
  
   /*Se deja pasar los datos de series con polizas montadas con siniestro o sin siniestro */
run;
  
%delete_table_db_ora(tabnm=TMP_PREP_PREV_R5_1_ENGINE, lib=&lib_resu);
%delete_table_db_ora(tabnm=TMP_PREP_PREV_R5_1_DETAIL, lib=&lib_resu);

data &lib_resu..TMP_PREP_PREV_R5_1_ENGINE;
	set lcasres.TMP_PREP_PREV_R5_1_ENGINE;
run;

data &lib_resu..TMP_PREP_PREV_R5_1_DETAIL;
	set lcasres.TMP_PREP_PREV_R5_1_DETAIL;
run;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");
	execute(delete from &schema_db2..QSC_PREP_PREV_R5_1_ENGINE where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	execute(delete from &schema_db2..QSC_PREP_PREV_R5_1_DETAIL where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");
	execute(insert into RESULTADOS.QSC_PREP_PREV_R5_1_ENGINE select 
		TRIM(SERIE_GR), TRIM(SERIE), TRIM(POLIZA), TRIM(ENDOSO), TRIM(INCISO), 
		TRIM(NOM_ASEG), TRIM(NOMBRE_ASEGURADO_GR), INI_VIG, FIN_VIG, FEC_EMI, 
		TRIM(NOM_AGTE), TRIM(NO_OFI), TRIM(SINIESTRO), TRIM(CODIGO_DE_PAGO), 
		TRIM(CVE_DOCTO), DIF_DIAS_INI_VIG1, DIAS_MONT1, SUM_IMPORTE_PAGO, 
		PAGOS_X_ASEGURADO, FLG_MONTADA, FLG_MISMO_ASEG2, FLG_SINIESTRADO, 
		TRIM(TIPO_PERSONA), PERIOD_BATCHDATE, sysdate from     
			&schema_db2.."TMP_PREP_PREV_R5_1_ENGINE") BY ORACLE;
	disconnect from oracle;
quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");
	execute(insert into RESULTADOS.QSC_PREP_PREV_R5_1_DETAIL select 
		TRIM(SERIE_GR), TRIM(SERIE), TRIM(POLIZA), TRIM(ENDOSO), TRIM(INCISO), 
		TRIM(NOM_ASEG), TRIM(NOMBRE_ASEGURADO_GR), INI_VIG, FIN_VIG, FEC_EMI, 
		TRIM(NOM_AGTE), TRIM(NO_OFI), TRIM(SINIESTRO), TRIM(CODIGO_DE_PAGO), 
		TRIM(CVE_DOCTO), DIF_DIAS_INI_VIG1, DIAS_MONT1, SUM_IMPORTE_PAGO, 
		PAGOS_X_ASEGURADO, FLG_MONTADA, FLG_MISMO_ASEG2, FLG_SINIESTRADO, 
		TRIM(TIPO_PERSONA), PERIOD_BATCHDATE, sysdate from     
			&schema_db2.."TMP_PREP_PREV_R5_1_DETAIL") BY ORACLE;
	disconnect from oracle;
quit;

%delete_table_db_ora(tabnm=TMP_PREP_PREV_R5_1_ENGINE, lib=&lib_resu);
%delete_table_db_ora(tabnm=TMP_PREP_PREV_R5_1_DETAIL, lib=&lib_resu);

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a 12 meses */
	execute(delete from &schema_db2..QSC_PREP_PREV_R5_1_ENGINE where 
		trunc(PERIOD_BATCHDATE) <= TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	execute(delete from &schema_db2..QSC_PREP_PREV_R5_1_DETAIL where 
		trunc(PERIOD_BATCHDATE) <= TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;

cas session_load terminate;