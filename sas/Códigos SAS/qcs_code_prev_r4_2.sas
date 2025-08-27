*************************************************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                                                              *
* FECHA: Septiembre 3, 2021                                                                                                         *
* TITULO: Creaci�n de Prep-Table Familia 4 para la regla 4.2                                                                        *
* SALIDAS: QCS_PREP_PREV_R4_2_ENGINE  *
* Modificación de codigo en regla 4.2, solicitada por Equipo siniestros PDF el 05/05/2023. 
* Se agregan los campos "CONDUCTOR, FECHA REPORTE y REPORTE" al detalle de las prep-table, que se modifican son:
* •  QCS_PREP_PREV_R4_2_ENGINE
* •  stg_pagosproveedores
* •  TMP_QCS_PREP_PREV_R4_2_ENGINE
* Modificación realizada por Juan Erik Perez (JEP) el 18/05/2023. 										 *

*************************************************************************************************************************************;



%include '/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_librerias_rutas2.sas';
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
data LISTA_COD_TXT_incluye LISTA_BENEFICIARIOS_excluye;
	set &lib_resu..QCS_PARAM_PREV_R4_2;

	if upcase(kstrip(PARAMETER_NAME))="VAR_NUMERO_PAGOS_X_BENEFICIARIO" then
		do;
			call symput("NUMERO_PAGOS_X_BENEFICIARIO", PARAMETER_VALUE);
		end;
	else if upcase(kstrip(PARAMETER_NAME))="LISTA_COD_TXT" and 
		missing(PARAMETER_VALUE) ne 1 then
			do;
			output LISTA_COD_TXT_incluye;
		end;
	else if upcase(kstrip(PARAMETER_NAME))="LISTA_BENEFICIARIOS" and 
		missing(PARAMETER_VALUE) ne 1 then
			do;
			output LISTA_BENEFICIARIOS_excluye;
		end;
run;

%put &=NUMERO_PAGOS_X_BENEFICIARIO;

/*Filtros *
Siniestros con código de text  where COD_TXT in (009 REEMBOLSO DE GRUAS ASEGURADO,010 REEMBOLSO DE GRUAS TERCERO, 011 REEMBOLSO DE CRISTAL ASEGURADO, 012 REEMBOLSO DE CONTRUCTORA ASEGURADO, 241 REEMBOLSO POR FALTANTES, 242 REEMBOLSO POR DOCTOS, 262 REEMBOLSO DE DEDUCIBLE, 263 REEMBOLSO AL ASEGURADO PERSONAS, 264 REEMBOLSO AL TERCERO, 285 REEMBOLSO AL ASEGURADO BIENES, 299, 331,332,333,334,335,336)
*/
proc sql;
	create table stg_pagosproveedores as select a.FECHA_PAGO	, kstrip(a.COD_TXT) as 
		COD_TXT 	, kstrip(a.DOCUMENTO_PAGO) as DOCUMENTO_PAGO	, kstrip(a.CVE_BENEF) as 
		CVE_BENEF 	, kstrip(a.BENEFICIARIO) as BENEFICIARIO	, kstrip(a.OFICINA_ATENCION) 
		as OFICINA_ATENCION	, kstrip(a.CODIGO_DE_PAGO) as CODIGO_DE_PAGO	, a.PK_FECOCU	, 
		a.FECHA_OCURRIDO	, kstrip(a.COBERTURA) as COBERTURA	, kstrip(a.ASEGURADO) as 
		ASEGURADO 	, 
		b.CONDUCTOR, 						/* Inclusion del campo CONDUCTOR � JEP 18/05/2023 */
		kstrip(a.POLIZA) as POLIZA 	, kstrip(a.INCISO) as INCISO 	, 
		kstrip(a.CLAVE_AGENTE) as CLAVE_AGENTE	, kstrip(a.AGENTE) as AGENTE, 
		kstrip(a.AJUSTADOR) as AJUSTADOR 	, kstrip(a.NRO_SINIESTRO) as SINIESTRO,
		b.REPORTE,  						/* Inclusion del campo REPORTE - JEP 18/05/2023 */
		b.FEC_HORA_REP, /* Inclusion del campo FECHA REPORTE - JEP 18/05/2023  se eliminó el formato, ya que presentaba errores -xm*/
		kstrip(a.DOCUMENTO_PAGO) as DOC_PAGO, a.IMPORTE_PAGO, kstrip(a.GRUPO_PROVEEDOR) AS 
		GRUPO_PROVEEDOR, kstrip(a.ENDOSO) as ENDOSO from
&lib_insu..pagosproveedores a left join LIB_INSU.APERTURA_REPORTE b 
on  a.NRO_SINIESTRO =(CATS(SUBSTR(b.REPORTE,1,4), b.SINIESTRO));
quit;

/*Identifica Flotilla  e individuales */
data poliza_flotilla;
	set stg_pagosproveedores;
	length INCISO2 $10.;
	INCISO2=compress(compress(INCISO), "0123456789", 'kis');
	INCISO_num=input(compress(INCISO2, , "kw") , best32.);

	if INCISO_num >  1 then
		do;
			output poliza_flotilla;
		end;
run;

/*Elimina la polizas de flotilla */
proc sql;
	create table pagosproveedores_ind as select b.* from stg_pagosproveedores b 
		where poliza not in (select poliza from poliza_flotilla) and 
		datepart(FECHA_PAGO) between  &fec_inicio and &fec_fin 
		and kstrip(upcase(DOCUMENTO_PAGO)) not in ('PT', 'SIPAC PAGO') and 
		UPCASE(KSTRIP(CODIGO_DE_PAGO))='I' and kstrip(COD_TXT) in (select 
		kstrip(PARAMETER_VALUE) from LISTA_COD_TXT_incluye);
quit;

data pagosproveedores_ind2 (drop=str_num leng_str_num PATTERN_asegurado);
	set pagosproveedores_ind;
	length beneficiario2 $300.;
	PATTERN_asegurado=PRXPARSE("/^[0-9]{10}/");
	valor_unico_pp=_N_;

	if (PRXMATCH(PATTERN_asegurado, BENEFICIARIO) > 0) then
		do;
			str_num=kstrip(scan(BENEFICIARIO, 1));
			leng_str_num=sum(length(str_num), 1);
			beneficiario2=kstrip(substr(BENEFICIARIO, leng_str_num));
			output pagosproveedores_ind2;
		end;
	else
		do;
			beneficiario2=BENEFICIARIO;
			output pagosproveedores_ind2;
		end;
run;

/*Excluye beneficiarios*/
/*Valida si existe lista de exclusión  */
%let num_benef_excluye= %get_num_obs(LISTA_BENEFICIARIOS_excluye);
%put  &=num_benef_excluye;

%macro excluye_beneficiario();
	%if %eval(&num_benef_excluye  >  0) %then
		%do;
			%put NOTE: excluye beneficiarios de pagosproveedores;

			/*Excluye lista de sucursales para PAGOSPROVEEDORES */
			data tmp_pagosproveedores_ind2;
				set pagosproveedores_ind2;
			run;

			proc sql;
				create table pagosproveedores_ind2 as select * from 
					tmp_pagosproveedores_ind2 where upcase(compress(beneficiario2, , "kw") ) 
					not in (select upcase(compress(PARAMETER_VALUE, , "kw")) from 
					LISTA_BENEFICIARIOS_excluye);
			quit;

		%end;
	%else
		%do;
			%put NOTE: NO excluye beneficiarios de pagosproveedores;
		%end;
%mend;

%excluye_beneficiario();

proc casutil OutCasLib='CasUser';
	load data=pagosproveedores_ind2 replace;
	quit;

data lcasres.pagosproveedores_ind3;
	set lcasres.pagosproveedores_ind2;
	length std_benef match_benef varchar(300);
	length tipo_persona varchar(10);
	benef_tipo_persona=dqidentify(beneficiario2, "Individual/Organization", 
		"ESMEX");

	if upcase(benef_tipo_persona) eq 'INDIVIDUAL' then
		do;
			tipo_persona='PF';
			std_benef=dqStandardize(beneficiario2, 'Name', 'ESMEX');
			match_benef=dqMatch(std_benef, 'Name', 95, 'ESMEX');
		end;
	else if upcase(benef_tipo_persona) eq 'ORGANIZATION' then
		do;
			tipo_persona='PM';
			std_benef=dqStandardize(beneficiario2, 'Organization', 'ESMEX');
			match_benef=dqMatch(std_benef, 'Organization', 95, 'ESMEX');
		end;
	else
		do;
			tipo_persona='UNKNOWN';
			std_benef=beneficiario2;
			match_benef=dqMatch(std_benef, 'Name', 95, 'ESMEX');
		end;
	len_std_benef=length(std_benef);
run;

proc cas;
	simple.groupByInfo / table={caslib="CasUser", name="pagosproveedores_ind3", 
		groupBy={"match_benef"}} copyVars={"beneficiario2", "std_benef", 
		"match_benef", "valor_unico_pp", "len_std_benef"} casOut={caslib="CasUser", 
		name="pagos_prov_grp", replace=true} includeDuplicates=true 
		generatedColumns={"FREQUENCY", "GROUPID", "POSITION"} details=true;
quit;

/*Obtiene Golden Record */
data lcasres.pagos_prov_surv;
	set lcasres.pagos_prov_grp (keep=_GROUPID_ _POSITION_ std_benef _Frequency_ 
		len_std_benef);
	by _GROUPID_ len_std_benef;
	length std_benef_surv varchar(300);

	if last._GROUPID_ then
		do;
			std_benef_surv=std_benef;
			output lcasres.pagos_prov_surv;
		end;
run;

proc fedsql sessref=session_load;
	create table pagos_prov_grp2 {OPTION replace=true replication=0} as select 
		b.*, s.std_benef_surv, s._POSITION_ as _POSITION_surv from pagos_prov_grp b 
		inner join pagos_prov_surv s on b._GROUPID_=s._GROUPID_;
	quit;

proc fedsql sessref=session_load;
	create table pagosproveedores_ind4  {OPTION replace=true replication=0} as 
		select b.*, c._GROUPID_ as ClusterId, c.std_benef_surv from 
		pagosproveedores_ind3 b left join pagos_prov_grp2 c on 
		b.valor_unico_pp=c.valor_unico_pp;
	quit;

proc fedsql sessref=session_load;
	/*Calculo de suma de  IMPORTE_PAGO por  beneficiario,poliza y SINIESTRO */
	create table sum_IMPORTE_PAGO {OPTION replace=true replication=0} as select 
		ClusterId, poliza, SINIESTRO, sum(IMPORTE_PAGO) as sum_IMPORTE_PAGO from 
		pagosproveedores_ind4 group by ClusterId, poliza, SINIESTRO;

	/*Calculo de numero de pagos por poliza*/
	create table sum_pagos_benef {OPTION replace=true replication=0} as select 
		ClusterId, poliza, count(distinct SINIESTRO) as cont_poliza_siniestro from 
		sum_IMPORTE_PAGO group by ClusterId, poliza;

	/*Calculo de numero de pagos por beneficiario*/
	create table sum_pagos_benef2 {OPTION replace=true replication=0} as select 
		ClusterId, sum(cont_poliza_siniestro) as sum_pagos_benef from sum_pagos_benef 
		group by ClusterId;
	quit;

proc fedsql sessref=session_load;
	create table sum_IMPORTE_PAGO2 {OPTION replace=true replication=0} as select 
		b.*, p.sum_pagos_benef from sum_IMPORTE_PAGO b left join sum_pagos_benef2 p 
		on b.ClusterId=p.ClusterId;
	quit;

	/*Une datos calculados a la tabla base */
proc fedsql sessref=session_load;
	create table pagosproveedores_ind5 {OPTION replace=true replication=0} as 
		select b.*, p.sum_IMPORTE_PAGO, p.sum_pagos_benef from pagosproveedores_ind4 
		b inner join sum_IMPORTE_PAGO2 p on b.ClusterId=p.ClusterId and 
		b.poliza=p.poliza and b.SINIESTRO=p.SINIESTRO;
	quit;

data lcasres.pagosproveedores_ind6;
	set lcasres.pagosproveedores_ind5;
	by ClusterId poliza SINIESTRO FECHA_PAGO;

	if last.SINIESTRO then
		do;
			output lcasres.pagosproveedores_ind6;
		end;
run;

/*********************************Obtiene datos de poliza *********************************/
proc sql;
	create table stg_produccion2_ as select kstrip(POLIZA) as POLIZA_PROD, 
		kstrip(ENDOSO) as ENDOSO_PROD, KSTRIP(INC) as INCISO_PROD, 
		upcase(kstrip(SERIE)) as serie, fec_emi, INI_VIG, FIN_VIG 
		from  &&lib_insu..PRODUCCION2 where missing(kstrip(POLIZA)) ne 1 and 
		kstrip(ENDOSO)='000000' and KSTRIP(INC)='0001';
quit;

proc sort data=stg_produccion2_;
	by poliza_prod endoso_prod inciso_prod serie fec_emi INI_VIG FIN_VIG;
	quit;

data stg_produccion2 dup_poliza_serie_fec_emi;
	set stg_produccion2_;
	by poliza_prod endoso_prod inciso_prod serie fec_emi INI_VIG FIN_VIG;

	if last.FIN_VIG then
		do;
			output stg_produccion2;
		end;

	if first.FIN_VIG=1 and last.FIN_VIG=1 then
		do;
		end;
	else
		do;
			output dup_poliza_serie_fec_emi;
		end;
run;

proc casutil outcaslib='CasUser';
	load data=work.stg_produccion2 casout="tmp_produccion2" replace;
	quit;

proc fedsql sessref=session_load;
	create table pagosproveedores_ind7  {OPTION replace=true replication=0} as 
		select b.*, p.POLIZA_PROD, p.INCISO_PROD, p.FEC_EMI, p.INI_VIG, p.FIN_VIG, 
		p.ENDOSO_PROD from pagosproveedores_ind6 b left join tmp_produccion2 p on 
		b.poliza=p.POLIZA_PROD;
	quit;

	/* Asigna flag de alerta */
data lcasres.TMP_QCS_PREP_PREV_R4_2_ENGINE
(keep=/*Datos pagosproveedores */
	FECHA_PAGO COD_TXT DOC_PAGO CVE_BENEF BENEFICIARIO OFICINA_ATENCION 
		FECHA_OCURRIDO COBERTURA ASEGURADO
		CONDUCTOR   				/* Inclusion del campo CONDUCTOR - JEP 18/05/2023 */	
		POLIZA INCISO AGENTE AJUSTADOR SINIESTRO
		REPORTE   					/* Inclusion del campo REPORTE - JEP 18/05/2023 */
		FEC_HORA_REP  				/* Inclusion del campo FECHA REPORTE - JEP 18/05/2023 */


		/*Cierra datos pagosproveedores */
		/*DAtos produccion2*/
		POLIZA_PROD INCISO_PROD FEC_EMI INI_VIG FIN_VIG ENDOSO_PROD

		/*Cierra datos produccion2*/
		/*Datos calculados */
		STD_BENEFICIARIO sum_IMPORTE_PAGO TIPO_PERSONA PAGOS_X_BENEFICIARIO 
		FLG_ALERT_PAGO CLUSTERID PERIOD_BATCHDATE

		/*Cierra datos calculados*/);
	set lcasres.pagosproveedores_ind7 (drop=BENEFICIARIO 
		rename=(BENEFICIARIO2=BENEFICIARIO sum_pagos_benef=PAGOS_X_BENEFICIARIO 
		std_benef_surv=STD_BENEFICIARIO tipo_persona=TIPO_PERSONA));

	if PAGOS_X_BENEFICIARIO  &NUMERO_PAGOS_X_BENEFICIARIO. then
		do;
			FLG_ALERT_PAGO=1;
		end;
	else
		do;
			FLG_ALERT_PAGO=0;
		end;
  

	PERIOD_BATCHDATE=DHMS(&fec_fin., 0, 0, 0);
	format PERIOD_BATCHDATE datetime20.;
run;

data lcasres.proveedores_x_pagos;
	set lcasres.tmp_QCS_PREP_PREV_R4_2_ENGINE (keep=beneficiario std_beneficiario 
		pagos_x_beneficiario flg_alert_pago);
	by std_beneficiario;

	if first.std_beneficiario and flg_alert_pago=1;
run;

%delete_table_db_ora(tabnm=TMP_QCS_PREP_PREV_R4_2_ENGINE, lib=&lib_resu);

/* Save the CAS tables to the database */
proc casutil incaslib="CasUser" outcaslib="QCS_Resultados_Planeacion";
	save casdata="TMP_QCS_PREP_PREV_R4_2_ENGINE" 
		casout="TMP_QCS_PREP_PREV_R4_2_ENGINE" 
		dataSourceOptions=(USENARROWCHARACTERTYPES=TRUE) replace;
	quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2
		path="&path_db2");
	execute(delete from &schema_db2..QCS_PREP_PREV_R4_2_ENGINE where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2
		path="&path_db2");
	execute(insert into RESULTADOS.QCS_PREP_PREV_R4_2_ENGINE select FECHA_PAGO, 
		COD_TXT, TRIM(DOC_PAGO), TRIM(CVE_BENEF), TRIM(BENEFICIARIO), 
		TRIM(OFICINA_ATENCION), FECHA_OCURRIDO, TRIM(COBERTURA), TRIM(ASEGURADO),
		TRIM(CONDUCTOR), 					/* Inclusion del campo CONDUCTOR - JEP 18/05/2023 */ 
		TRIM(POLIZA), TRIM(INCISO), TRIM(AGENTE), TRIM(AJUSTADOR), TRIM(SINIESTRO),
		TRIM(REPORTE), 						/* Inclusion del campo REPORTE - JEP 18/05/2023 */
		FEC_HORA_REP, 						/* Inclusion del campo FECHA REPORTE - JEP 18/05/2023 */
		TRIM(POLIZA_PROD), TRIM(INCISO_PROD), FEC_EMI, INI_VIG, FIN_VIG, 
		TRIM(ENDOSO_PROD), TRIM(STD_BENEFICIARIO), SUM_IMPORTE_PAGO, 
		TRIM(TIPO_PERSONA), PAGOS_X_BENEFICIARIO, FLG_ALERT_PAGO, CLUSTERID, sysdate, 
		PERIOD_BATCHDATE from     
		&schema_db2.."TMP_QCS_PREP_PREV_R4_2_ENGINE") BY ORACLE;
	disconnect from oracle;
quit;

%delete_table_db_ora(tabnm=TMP_QCS_PREP_PREV_R4_2_ENGINE, lib=&lib_resu);

/*Solo se dejan doce meses en la historia*/
proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a 12 meses */
	execute(delete from &schema_db2..QCS_PREP_PREV_R4_2_ENGINE where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;

cas session_load terminate;