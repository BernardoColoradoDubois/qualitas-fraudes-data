%include '/qcs/projects/default/qcsrun/sas/programs/prevencion/qcs_code_prev_librerias_rutas2.sas';
%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macros_utility_prev.sas';
 
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

/*Lectura de parámetros */
data exc_LISTA_OFICINA_DM exc_LISTA_OFICINA_PT;
	set &lib_resu..QCS_PARAM_PREV_R3_1;

	if PARAMETER_NAME="VAR_PORC_DEDUCIBLE_INGRESADO_PT" then
		do;
			call symput("PORC_DEDUCIBLE_INGRESADO_PT", PARAMETER_VALUE);
		end;
	else if PARAMETER_NAME="VAR_PORC_DEDUCIBLE_INGRESADO_DM" then
		do;
			call symput("PORC_DEDUCIBLE_INGRESADO_DM", PARAMETER_VALUE);
		end;
	else if PARAMETER_NAME="LISTA_OFICINA_DM" and missing(PARAMETER_VALUE) ne 1 
		then
			do;
			output exc_LISTA_OFICINA_DM;
		end;
	else if PARAMETER_NAME="LISTA_OFICINA_PT" and missing(PARAMETER_VALUE) ne 1 
		then
			do;
			output exc_LISTA_OFICINA_PT;
		end;
run;

%put PORC_DEDUCIBLE_INGRESADO_PT: &PORC_DEDUCIBLE_INGRESADO_PT;
%put PORC_DEDUCIBLE_INGRESADO_DM: &PORC_DEDUCIBLE_INGRESADO_DM;

/*Obtiene datos de APERCAB_BSC */
proc sql;
	create table stg_apercab_bsc as select kstrip(POLIZA) as POLIZA, 
		kstrip(SINIESTRO) as SINIESTRO , TIP_RESP_ORI as TIP_RESP_ORI, 
		kstrip(REPORTE) as REPORTE , FEC_OCU, upcase(kstrip(SERIE_VEH)) as serie, 
		kstrip(asegurado) as asegurado, kstrip(cod_resp) as cod_resp, 
		upcase(kstrip(tipo_robo)) as tipo_robo from &lib_insu..APERCAB_BSC where 
		datepart(FEC_OCU) between &fec_inicio. and  &fec_fin.
		    and missing(siniestro)=0 and missing(poliza)=0
/* 		Excluir siniestros con recuperación entre compañías FL 03-10-23 */
		and REPORTE not in (select distinct REPORTE from &lib_insu..ORDENES_BSC)
/* 		Excluir siniestros con intervención de Jurídico FL 03-10-23*/
		and missing(CVE_ABOGADO) = 1;
quit;

data stg_apercab_cod_rp stg_apercab_rt;
	set stg_apercab_bsc;
	val_unique_apercab=_N_;

	if TIP_RESP_ORI in ('01', '07', '13') then
		do;
			output stg_apercab_cod_rp;
		end;

	if tipo_robo='ROBO TOTAL' then
		do;
			output stg_apercab_rt;
		end;
run;

/*Obtiene datos de Valuacion */
proc sql;
	create table stg_valuacion as select kstrip(POLIZA) as POLIZA, kstrip(INCISO) 
		as INCISO , kstrip(SINIESTRO) as SINIESTRO_VAL, KSTRIP(ASEG_TERCERO) as 
		ASEG_TERCERO, TOTAL_VALUACION, IMPORTE_PT, DEDUCIBLE, SUMAASEG, NUM_PT, 
		NUM_DEDUCIBLES, FECHA_ENTREGA, FECHA_OCURRIDO, fecha_valuacion, 
		kstrip(ESTATUS_PROCESO) as ESTATUS_PROCESO, kstrip(COBERTURA) as 
		COBERTURA_VAL, FECHA_CAPTURA, upcase(kstrip(serie)) as serie_val 
		from  &lib_insu..VALUACION where UPCASE(KSTRIP(ASEG_TERCERO))='ASEGURADO';
quit;

data valuacion_pt_0 valuacion_pt1;
	set stg_valuacion;

	if NUM_PT=1 then
		do;
			output valuacion_pt1;
		end;
	else if NUM_PT=0 and NUM_DEDUCIBLES=> 1 and missing(FECHA_ENTREGA)=0 then
		do;
			output valuacion_pt_0;
		end;
run;

/**************** Obtiene datos de PAGOSPROVEEDORES /****************/
Proc sql;
	create table stg_PAGOSPROVEEDORES as select kstrip(NRO_SINIESTRO) as 
		NRO_SINIESTRO , kstrip(DOCUMENTO_PAGO) as DOCUMENTO_PAGO , COBERTURA as 
		COBERTURA_ANT, case when COBERTURA IN ('DANOS MAT', 'DANOS MAT.', 
		'DANOS.MAT') then 'DANOS MAT' when COBERTURA in ('ROBO TOTAL', 'ROBO TOTAL.', 
		'ROBO.TOTAL') then 'ROBO TOTAL' else '' end as std_cobertura, CODIGO_DE_PAGO, 
		FECHA_PAGO, IMPORTE_PAGO, POLIZA, ENDOSO, INCISO, kstrip(OFICINA_ATENCION) as 
		OFICINA_ATENCION from  &lib_insu..PAGOSPROVEEDORES where 
		upcase(kstrip(DOCUMENTO_PAGO))='PT' and upcase(kstrip(cobertura)) 
		IN ('DANOS MAT', 'DANOS MAT.', 'DANOS.MAT', 'ROBO TOTAL', 'ROBO TOTAL.', 
		'ROBO.TOTAL') and UPCASE(KSTRIP(CODIGO_DE_PAGO))='I';
quit;

/*Excluye sucursales*/
/*Valida si existe lista de exclusión  */
  %let num_suc_excluye= %get_num_obs(exc_LISTA_OFICINA_PT);
%put  &=num_suc_excluye;

%macro excluye_suc();
	%if  &num_suc_excluye  >  0 %then
		%do;
			%put excluye sucursales de pagosproveedores;

			/*Excluye lista de sucursales para PAGOSPROVEEDORES */
			data tmp_stg_PAGOSPROVEEDORES;
				set stg_PAGOSPROVEEDORES;
			run;

			proc sql;
				create table stg_PAGOSPROVEEDORES as select * from tmp_stg_PAGOSPROVEEDORES 
					where upcase(compress(OFICINA_ATENCION, , "kw") ) not in (select 
					upcase(compress(PARAMETER_VALUE, , "kw")) from exc_LISTA_OFICINA_PT);
			quit;

		%end;
	%else
		%do;
			%put NO excluye sucursales;
		%end;
%mend;

%excluye_suc();

/*Calculo del monto de IMPORTE_PAGO, agrupado por  poliza y siniestro*/
proc sql;
	create table PAGOSPROVE_imp as select POLIZA, NRO_SINIESTRO, sum(IMPORTE_PAGO) 
		as sum_importe_pago from stg_PAGOSPROVEEDORES group by POLIZA, NRO_SINIESTRO 
		order by POLIZA, NRO_SINIESTRO;
quit;

/*Union de importe calculado con la tabla de stg_PAGOSPROVEEDORES*/
proc sql;
	create table tmp_PAGOSPROVEEDORES as select p.POLIZA, p.NRO_SINIESTRO, 
		DOCUMENTO_PAGO, std_cobertura, CODIGO_DE_PAGO, OFICINA_ATENCION, FECHA_PAGO, 
		sum_importe_pago,
        endoso, inciso 
    from stg_PAGOSPROVEEDORES p inner join PAGOSPROVE_imp i on 
		p.poliza=i.poliza and p.NRO_SINIESTRO=i.NRO_SINIESTRO;
quit;

/*
Se deja la oficina de atención obtenidad por la ultima fecha de pago
*/
proc sort data=tmp_PAGOSPROVEEDORES;
	by std_cobertura POLIZA NRO_SINIESTRO descending FECHA_PAGO;
	quit;

proc sort data=tmp_PAGOSPROVEEDORES nodupkey;
	by POLIZA NRO_SINIESTRO;
	quit;

/**************** Obtiene datos de Produccion2 con   kstrip(ENDOSO)="000000"****************/
proc sql;
	create table stg_produccion2_ as select kstrip(P.POLIZA) as POLIZA, 
		kstrip(P.ENDOSO) as ENDOSO, KSTRIP(P.INC) as INCISO, S.SUMA_ASEGURADA as SUMA_ASEG, P.PORC_DM, P.PORC_RT, 
		P.PRIMA_EXDED, P.PRIMA_CADE, upcase(kstrip(P.SERIE)) as serie, P.fec_emi, P.INI_VIG, 
		P.FIN_VIG from  &lib_insu..PRODUCCION2 P
/* 		Obtenemos la suma asegurada de la base SUMAS_ASEGURADAS FL 03-10-23*/
		inner join &lib_insu..SUMAS_ASEGURADAS S
			on P.SERIE = S.SERIE
			and P.POLIZA = S.POLIZA
			and P.ENDOSO = S.ENDOSO
			and P.INC = S.INCISO
		where kstrip(P.ENDOSO)="000000";
quit;

/*Dado hubiera duplicados se queda con la ultima fecha de emision e inicio de vigencia */
proc sort data=stg_produccion2_;
	by poliza endoso inciso serie fec_emi INI_VIG;
	quit;

data stg_produccion2 dup_poliza_serie_fec_emi;
	set stg_produccion2_;
	by poliza endoso inciso serie fec_emi INI_VIG;

	if last.INI_VIG then
		do;
			output stg_produccion2;
		end;

	if first.INI_VIG=1 and last.INI_VIG=1 then
		do;
		end;
	else
		do;
			output dup_poliza_serie_fec_emi;
		end;
run;
/**************** Termina los datos de Produccion2 con   kstrip(ENDOSO)="000000"****************/

/**************** Obtiene datos de Produccion2 con todos los endosos"***************/
proc sql;
	create table stg_produccion2_2_ as select kstrip(P.POLIZA) as POLIZA, 
		kstrip(P.ENDOSO) as ENDOSO, KSTRIP(P.INC) as INCISO, S.SUMA_ASEGURADA as SUMA_ASEG, P.PORC_DM, P.PORC_RT, 
		P.PRIMA_EXDED, P.PRIMA_CADE, upcase(kstrip(P.SERIE)) as serie, P.fec_emi, P.INI_VIG, 
		P.FIN_VIG from  &lib_insu..PRODUCCION2 P
/* 		Obtenemos la suma asegurada de la base SUMAS_ASEGURADAS FL 03-10-23*/
		inner join &lib_insu..SUMAS_ASEGURADAS S
			on P.SERIE = S.SERIE
			and P.POLIZA = S.POLIZA
			and P.ENDOSO = S.ENDOSO
			and P.INC = S.INCISO
/* 		Excluimos aquellos que tienen dispositivo satelital */
		where P.POLIZA not in (select POL from &lib_insu..COBRANZA where missing(SERIE_SAT) = 0);
quit;

/*Dado hubiera duplicados se queda con la ultima fecha de emision e inicio de vigencia */
proc sort data=stg_produccion2_2_;
	by poliza endoso inciso serie fec_emi INI_VIG;
	quit;

data stg_produccion2_2 dup_poliza_serie_fec_emi2;
	set stg_produccion2_2_;
	by poliza endoso inciso serie fec_emi INI_VIG;

	if last.INI_VIG then
		do;
			output stg_produccion2_2;
		end;

	if first.INI_VIG=1 and last.INI_VIG=1 then
		do;
		end;
	else
		do;
			output dup_poliza_serie_fec_emi2;
		end;
run;

/**************** Termina datos de Produccion2 con todos los endosos"****************/


/****************Inicia calculo de deducible para ROBO TOTAL Y Daños Materiales PT (PT=1) ******************/
proc sql;
	create table apercab_val_pt1 as select b.*, v.inciso, v.ASEG_TERCERO, 
		v.TOTAL_VALUACION, v.IMPORTE_PT, v.DEDUCIBLE, v.SUMAASEG, v.NUM_PT, 
		v.NUM_DEDUCIBLES, v.FECHA_ENTREGA, v.FECHA_OCURRIDO, v.fecha_valuacion, 
		v.ESTATUS_PROCESO, V.COBERTURA_VAL, V.FECHA_CAPTURA, v.serie_val from 
		stg_apercab_cod_rp b inner join valuacion_pt1 v on b.poliza=v.poliza and 
		b.siniestro=v.SINIESTRO_VAL;
quit;

/*Manejo de registos duplicados de valuacion*/
proc sort data=apercab_val_pt1;
	by val_unique_apercab;
	quit;

data dup_apercab_val_pt1;
	set apercab_val_pt1;

	if COBERTURA_VAL='01';
run;

proc sort data=dup_apercab_val_pt1;
	by poliza siniestro descending FECHA_CAPTURA;
	quit;

proc sort data=dup_apercab_val_pt1 nodupkey;
	by poliza siniestro;
	quit;

	/*Validación de Serie*/
	/*Valida SERIE
	•	Regla:
	Para los siniestros con daños materiales de pérdida total(pt=1), después de cruzar las base de Apercab y Valuación por póliza-siniestro, validar que los valores de los
	campos de serie “APERCAB_BSC.serie_veh” y  “VALUACION. Serie” sean iguales, si no son iguales eliminar el registro.
	*/
data ndup_apercab_val_pt1;
	set dup_apercab_val_pt1;

	if upcase(kstrip(serie)) eq upcase(kstrip(serie_val));
run;

/*Elimina duplicados por póliza y siniestro para robo total*/
proc sort data=stg_apercab_rt;
	by poliza siniestro descending FEC_OCU;
	quit;

proc sort data=stg_apercab_rt nodupkey;
	by poliza siniestro;
	quit;

	/*se agrega datos de pt=1 con datos de robo total*/
proc append base=ndup_apercab_val_pt1 data=stg_apercab_rt force;
	quit;

proc sql;
	/*Union de datos de PAGOSPROVEEDORES  */
	create table cab_pagof_DM_PT1_RT as select b.*, p.DOCUMENTO_PAGO, 
		p.CODIGO_DE_PAGO, p.OFICINA_ATENCION, p.sum_importe_pago, p.std_cobertura, 
        p.endoso as endoso_pagos_prove,p.inciso as inciso_pagos_prove
		from ndup_apercab_val_pt1 b inner join tmp_PAGOSPROVEEDORES p on 
		b.POLIZA=p.POLIZA and b.SINIESTRO=p.NRO_SINIESTRO;
quit;
  
/*Union de datos de produccion2   
Regla:Para tomar la suma asegurada correcta se debe formar la llave Poliza+endoso+inciso de la tabla pagos proveedores
y esta buscarla en Produccion2, por lo que en produccion2 debe considerar todos los endosos 
*/         
proc sql;
	create table cab_pago_prod_DM_PT1_RT as select b.*, p.SUMA_ASEG, p.PORC_DM, 
		p.PORC_RT, p.PRIMA_EXDED, p.PRIMA_CADE, p.ENDOSO, p.INI_VIG, p.FIN_VIG
        from 
		cab_pagof_DM_PT1_RT b inner join stg_produccion2_2 p 
		on   b.serie=   p.serie
        and  b.poliza=  p.poliza
        and  b.endoso_pagos_prove = p.ENDOSO
        and  b.inciso_pagos_prove = p.INCISO
     order by serie, poliza, endoso_pagos_prove, inciso_pagos_prove;
quit;

data cab_pago_prod_DM_X_PT1_RT exc_exded_cade_DM_PT otros;
	set cab_pago_prod_DM_PT1_RT;

	if PRIMA_EXDED >=1 or PRIMA_CADE >=1 then
		do;
			output exc_exded_cade_DM_PT;
		end;
	else
		do;

			if std_cobertura eq "DANOS MAT" then
				do;

					/*Formula del calculo del deducible para "DANOS MAT
					a) Calculo de deducible =Produccion2.suma_aseg  * Produccion2.porc_dm
					b) Deducible neteado = Produccion2.suma_aseg -  ∑  pagosproveedores.importe_pago   /*agrupa por poliza, siniestro
					c) Comparación Deducible =  b / a
					*/
					deducible_cal_pt=suma_aseg * (porc_dm / 100);
					deducible_neteado=sum(suma_aseg, - sum_importe_pago);
					comp_Deducible=ifn(deducible_cal_pt=0 or deducible_cal_pt=. , ., 
						deducible_neteado/deducible_cal_pt);
					deducible_fal=deducible_cal_pt - deducible_neteado;
					output cab_pago_prod_DM_X_PT1_RT;
				end;

			if std_cobertura eq "ROBO TOTAL" then
				do;

					/*Formula del calculo del deducible para "ROBO TOTAL"
					a) Calculo de deducible =Produccion2.suma_aseg  * Produccion2.porc_rt
					b) Deducible neteado = Produccion2.suma_aseg -  ∑  pagosproveedores.importe_pago   /*agrupa por poliza, siniestro
					c) Comparación Deducible =  b / a
					*/
					deducible_cal_rt=ifn(PORC_RT=0 or PORC_RT=., ., suma_aseg * (PORC_RT / 
						100));
					deducible_neteado=sum(suma_aseg, - sum_importe_pago);
					comp_Deducible=ifn(deducible_cal_rt=0 or deducible_cal_rt=. , ., 
						deducible_neteado/deducible_cal_rt);
					deducible_fal=deducible_cal_rt - deducible_neteado;
					output cab_pago_prod_DM_X_PT1_RT;
				end;

			if std_cobertura ne "DANOS MAT" and std_cobertura ne "ROBO TOTAL" then
				do;
					output otros;
				end;
		end;
run;

data motora_dm
	(keep=/*DAtos apercab*/
	siniestro poliza asegurado TIP_RESP_ORI REPORTE TIPO_ROBO FEC_OCU cod_resp

	/*Cierra datos apercab*/
	/*Datos valuación */
	INCISO ASEG_TERCERO TOTAL_VALUACION IMPORTE_PT deducible SUMAASEG NUM_PT 
		NUM_DEDUCIBLES FECHA_ENTREGA ESTATUS_PROCESO

		/*Cierra datos de Valuacion*/
		/*Datos produccion2 */
		ENDOSO SUMA_ASEG PORC_DM PORC_RT INI_VIG FIN_VIG

		/*Cierra datos de produccion2*/
		/*Datos PAGOSPROVEEDORES*/
		DOCUMENTO_PAGO cobertura CODIGO_DE_PAGO IMPORTE_PAGO OFICINA_ATENCION

		/*Cierra datos de PAGOSPROVEEDORES*/
		/*Datos calculados */
		deducible_pt deducible_rt deducible_neteado compara_deducible deduc_dm 
		flg_alert deducible_fal PERIOD_BATCHDATE BATCHDATE

		/*Cierra datos calculados */);
	set cab_pago_prod_DM_X_PT1_RT
    (rename=(std_cobertura=cobertura sum_importe_pago=importe_pago 
		deducible_cal_pt=deducible_pt deducible_cal_rt=deducible_rt 
		comp_Deducible=compara_Deducible));
	deduc_dm=deducible_pt;

	if missing(compara_Deducible)=0 and 
		compara_Deducible  &PORC_DEDUCIBLE_INGRESADO_PT.  then
			do;
			flg_alert=1;
		end;
	else
		do;
			flg_alert=0;
		end;
	PERIOD_BATCHDATE=DHMS(&fec_fin., 0, 0, 0);
	BATCHDATE=DATETIME();
	format PERIOD_BATCHDATE BATCHDATE datetime20.;
run;

data dm_detalle (keep=OFICINA_ATENCION siniestro poliza asegurado SUMA_ASEG 
		PORC_DM PORC_RT cobertura importe_pago deduc_dm deducible_rt deducible_pt 
		deducible_neteado compara_Deducible deducible_fal flg_alert PERIOD_BATCHDATE 
		BATCHDATE);
	set motora_dm;
run;

/****************Termina calculo de deducible para ROBO TOTAL Y Daños Materiales PT (PT=1) ******************/
/****************Inicia calculo de deducible  NUM_PT = 0  **********************/
/*Union de apercab con valuación */
proc sql;
	create table apercab_val_DM0 as select b.*, v.inciso, v.ASEG_TERCERO, 
		v.TOTAL_VALUACION, v.IMPORTE_PT, v.DEDUCIBLE, v.SUMAASEG, v.NUM_PT, 
		v.NUM_DEDUCIBLES, v.FECHA_ENTREGA, v.FECHA_OCURRIDO, v.fecha_valuacion, 
		v.ESTATUS_PROCESO, V.COBERTURA_VAL, V.FECHA_CAPTURA, v.serie_val from 
		stg_apercab_cod_rp b inner join valuacion_pt_0 v on b.poliza=v.poliza and 
		b.siniestro=v.SINIESTRO_VAL;
quit;

/*Manejo de registos duplicados por dup_apercab_val_DM0 */
data dup_apercab_val_DM0;
	set apercab_val_DM0;

	if COBERTURA_VAL='01';
run;

data dup_apercab_val_DM0;
	set dup_apercab_val_DM0;
	tmp_sinies_aper=input(siniestro, 15.);
run;

proc sort data=dup_apercab_val_DM0;
	by tmp_sinies_aper FECHA_CAPTURA;
	quit;

data dup_apercab_val_DM0;
	set dup_apercab_val_DM0;
	by tmp_sinies_aper FECHA_CAPTURA;

	if last.tmp_sinies_aper;
run;

/*Valida SERIE
•	Regla:
Para los siniestros con daños materiales sin pérdida total(pt=0), después de cruzar las base de Apercab y Valuación por póliza-siniestro, validar que los valores de los
campos de serie “APERCAB_BSC.serie_veh” y  “VALUACION. Serie” sean iguales, si no son iguales eliminar el registro.
*/
data dup_apercab_val_DM0;
	set dup_apercab_val_DM0;

	if upcase(kstrip(serie))=upcase(kstrip(serie_val));
run;

/*Union por Poliza,serie */
/*
proc sql;
	create table apercab_val_prod_DM0 as select b.*, p.SUMA_ASEG, p.PORC_DM, 
		p.ENDOSO, p.PORC_RT, p.INI_VIG, p.FIN_VIG, p.inciso as inciso_prod, 
		p.PRIMA_EXDED, p.PRIMA_CADE from dup_apercab_val_DM0 b inner join 
		stg_produccion2 p on b.poliza=p.poliza and b.serie_val=p.serie;
quit;
*/

/*Union por Poliza,serie e inciso  */
proc sql;
	create table apercab_val_prod_DM0 as select b.*, p.SUMA_ASEG, p.PORC_DM, 
		p.ENDOSO, p.PORC_RT, p.INI_VIG, p.FIN_VIG, p.inciso as inciso_prod, 
		p.PRIMA_EXDED, p.PRIMA_CADE from dup_apercab_val_DM0 b inner join 
		stg_produccion2 p on b.poliza=p.poliza and b.serie_val=p.serie
        and  p.inciso=b.inciso;
quit;

data apercab_val_prod2_DM0 exc_EXDED_CADE_DM0;
	set apercab_val_prod_DM0;

	if PRIMA_EXDED >=1 or PRIMA_CADE >=1 then
		do;
			output exc_EXDED_CADE_DM0;
		end;
	else
		do;
			output apercab_val_prod2_DM0;
		end;
run;

proc sql;
	/* Obtiene datos de recuperaciones */
	create table stg_recuperaciones as select kstrip(poliza) as poliza, ramo, 
		anio, siniestro, tipo, cats(kstrip(ramo), kstrip(anio), kstrip(siniestro)) AS 
		std_siniestro length=20, monto_ingreso, cobertura, case when 
		upcase(kstrip(cobertura)) in ('DANOS MAT', 'DANOS MAT.', 'DM') then 
		'DANOS MAT' else '' end as std_cobertura_rec , kstrip(OFI_ATENCION) as 
		OFI_ATENCION, datepart(fecha_de_ocurrido) as fecha_de_ocurrido, fecha_ingreso, MON_ING,T_CAMBIO 
		from  &lib_insu..recuperaciones where UPCASE(kstrip(tipo))='DEDUCIBLE' and 
		upcase(kstrip(cobertura)) in ('DANOS MAT', 'DANOS MAT.', 'DM');
quit;

/*Convertierte todos las recuperaciones ingresadas a moneda Nacional*/
data stg_recuperaciones (drop= tmp_MONTO_INGRESO  MON_ING T_CAMBIO  );
      set  stg_recuperaciones (rename=(monto_ingreso=tmp_MONTO_INGRESO) );

	if upcase(kstrip(MON_ING)) eq 'DLS' then
		do;
			MONTO_INGRESO=tmp_MONTO_INGRESO * T_CAMBIO;

			if  round(T_CAMBIO, .000001) <= 0 or  missing(T_CAMBIO) = 1  then
				do;
					put "Nota: MONTO_INGRESO=" tmp_MONTO_INGRESO "Tipo de cambio <= 0   T_CAMBIO=" T_CAMBIO;
				end;
        end;
	    else 
				do;
					MONTO_INGRESO=tmp_MONTO_INGRESO;
				end;
run;


/*Excluye sucursales*/
/*Valida si existe lista de exclusión */
  %let num_suc_excluye= %get_num_obs(exc_LISTA_OFICINA_DM);
%put  &=num_suc_excluye;

%macro excluye_suc();
	%if  &num_suc_excluye  >  0 %then
		%do;
			%put Excluye lista de sucursales de la base de recuperaciones ;

			/*Excluye lista de sucursales para base de recuperaciones */
			data tmp_stg_recuperaciones;
				set stg_recuperaciones;
			run;

			proc sql;
				create table stg_recuperaciones as select * from tmp_stg_recuperaciones 
					where upcase(compress(OFI_ATENCION, , "kw") ) not in (select 
					upcase(compress(PARAMETER_VALUE, , "kw")) from exc_LISTA_OFICINA_DM);
			quit;

		%end;
	%else
		%do;
			%put NO excluye sucursales;
		%end;
%mend;

%excluye_suc();

proc sql;
	/*Obtiene monto de de ingreso por poliza y siniestro */
	create table grp_recuperacion_ingreso as select poliza, std_siniestro, 
		sum(monto_ingreso) as sum_monto_ingreso from stg_recuperaciones group by 
		poliza, std_siniestro order by poliza, std_siniestro;

	/*Obtiene datos de la cobertura */
	create table grp_recuperacion_cober as select b.ramo, b.anio, b.tipo, 
		b.siniestro, b.fecha_de_ocurrido, b.fecha_ingreso, b.poliza, b.std_siniestro, 
		b.std_cobertura_rec, b.OFI_ATENCION as OFICINA_ATENCION_2, 
		m.sum_monto_ingreso from stg_recuperaciones b inner join 
		grp_recuperacion_ingreso m on b.poliza=m.poliza and 
		b.std_siniestro=m.std_siniestro;
quit;

proc sql;
	/*
	Lista de campos a los cuales se realiza la operación de distinct
	ramo, anio,siniestro,tipo, cobertura,  datepart(Fecha_de_ocurrido),poliza,ofi_atencion
	*/
	create table grp_recuperacion_cober2 as select distinct ramo, anio, siniestro, 
		tipo, Fecha_de_ocurrido, poliza, OFICINA_ATENCION_2, std_siniestro, 
		std_cobertura_rec, sum_monto_ingreso from grp_recuperacion_cober;
quit;

proc sql;
	/*Union de datos de recuperaciones por siniestro y poliza */
	create table apercab_val_prod_rec_DM0 as select b.*, r.sum_monto_ingreso, 
		r.std_cobertura_rec, r.OFICINA_ATENCION_2 from apercab_val_prod2_DM0 b inner 
		join grp_recuperacion_cober2 r on b.poliza=r.poliza and 
		b.siniestro=r.std_siniestro;
quit;

data tmp_motora_calculo_pt0;
	set apercab_val_prod_rec_DM0;

	/*
	a) Calculo de diferencia = valuacion.DEDUCIBLE  - recuperaciones.monto_ingreso
	*/
	dif_de_ingreso=sum(DEDUCIBLE, - sum_monto_ingreso);

	/*
	b) Comparación Deducible =   a / valuacion.DEDUCIBLE
	*/
	comp_deducible=ifn(dif_de_ingreso=0 or dif_de_ingreso=. , ., (dif_de_ingreso / 
		DEDUCIBLE) );
run;

/*datos de salida para PT=0*/
data motora_dm_pt0
(keep=dif_de_ingreso compara

	/*DAtos apercab*/
	asegurado poliza siniestro TIP_RESP_ORI REPORTE TIPO_ROBO FEC_OCU cod_resp

	/*Cierra datos apercab*/
	/*Datos valuación */
	INCISO ASEG_TERCERO TOTAL_VALUACION IMPORTE_PT deducible SUMAASEG NUM_PT 
		NUM_DEDUCIBLES FECHA_ENTREGA ESTATUS_PROCESO

		/*Cierra datos de Valuacion*/
		/*Datos produccion2 */
		ENDOSO SUMA_ASEG PORC_DM PORC_RT INI_VIG FIN_VIG

		/*Cierra datos de produccion2*/
		/*Datos PAGOSPROVEEDORES  */
		OFICINA_ATENCION_2

		/*Cierra datos de PAGOSPROVEEDORES*/
		/*DAtos recuperaciones */
		cobertura OFICINA_ATENCION_2 sum_m_ingreso

		/*Cierra datos de recuperaciones */
		/*Datos calculados*/
		dif_de_ingreso compara flg_alert PERIOD_BATCHDATE BATCHDATE

		/*Cierra datos calculados */);
	set tmp_motora_calculo_pt0 (rename=(std_cobertura_rec=cobertura 
		sum_monto_ingreso=sum_m_ingreso comp_deducible=compara) );

	/*
	b) Comparación Deducible =   a / valuacion.DEDUCIBLE
	Alerta siniestros con deducible ingresado menor al 70%
	Se alerta si b) es mayor a 0.3
	*/
	if missing(compara)=0 and compara  &PORC_DEDUCIBLE_INGRESADO_DM.  then
		do;
			flg_alert=1;
		end;
	else
		do;
			flg_alert=0;
		end;
	PERIOD_BATCHDATE=DHMS(&fec_fin., 0, 0, 0);
	BATCHDATE=DATETIME();
	format PERIOD_BATCHDATE BATCHDATE datetime20.;
run;

data dm_pt0_detalle(keep=OFICINA_ATENCION_2 siniestro poliza asegurado 
		SUMA_ASEG PORC_DM PORC_RT cobertura sum_m_ingreso deducible dif_de_ingreso 
		compara flg_alert PERIOD_BATCHDATE BATCHDATE);
	set motora_dm_pt0;
run;

proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2
		path="&path_db2");
	execute(delete from  &schema_db2..QCS_PREP_PREV_R3_1_ENGINE_PT where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	execute(delete from  &schema_db2..QCS_PREP_PREV_R3_1_DETAIL_PT where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	execute(delete from  &schema_db2..QCS_PREP_PREV_R3_1_ENGINE_DM where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	execute(delete from  &schema_db2..QCS_PREP_PREV_R3_1_DETAIL_DM where 
		trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS')) 
		) BY ORACLE;
	disconnect from oracle;
quit;

proc append base=&lib_resu..QCS_PREP_PREV_R3_1_ENGINE_PT data=motora_dm force;
	quit;

proc append base=&lib_resu..QCS_PREP_PREV_R3_1_DETAIL_PT data=dm_detalle force;
	quit;

proc append base=&lib_resu..QCS_PREP_PREV_R3_1_ENGINE_DM data=motora_dm_pt0 
		force;
	quit;

proc append base=&lib_resu..QCS_PREP_PREV_R3_1_DETAIL_DM data=dm_pt0_detalle 
		force;
	quit;

	/*Solo se dejan doce meses en la historia*/
proc sql;
	connect to oracle (user=&user_db2
		orapw=&pwd_user_db2 path="&path_db2");

	/*Borra datos históricos mayores a 12 meses */
	execute(delete from &schema_db2..QCS_PREP_PREV_R3_1_ENGINE_PT where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	execute(delete from &schema_db2..QCS_PREP_PREV_R3_1_DETAIL_PT where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	execute(delete from &schema_db2..QCS_PREP_PREV_R3_1_ENGINE_DM where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	execute(delete from &schema_db2..QCS_PREP_PREV_R3_1_DETAIL_DM where 
		trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora, 
		'YYYY-MM-DD HH24:MI:SS')) ) BY ORACLE;
	disconnect from oracle;
quit;

/****************termina calculo de deducible para  NUM_PT = 0  ****************/

proc datasets lib = WORK nolist nowarn memtype = (data view);
   delete  APERCAB_VAL_DM0 APERCAB_VAL_PROD2_DM0
   APERCAB_VAL_PROD_DM0
APERCAB_VAL_PROD_REC_DM0
APERCAB_VAL_PT1
CAB_PAGOF_DM_PT1_RT
CAB_PAGO_PROD_DM_PT1_RT
CAB_PAGO_PROD_DM_X_PT1_RT
DM_DETALLE
DM_PT0_DETALLE
DUP_APERCAB_VAL_DM0
DUP_APERCAB_VAL_PT1
DUP_POLIZA_SERIE_FEC_EMI
DUP_POLIZA_SERIE_FEC_EMI2
EXC_EXDED_CADE_DM0
EXC_EXDED_CADE_DM_PT
EXC_LISTA_OFICINA_DM
EXC_LISTA_OFICINA_PT
GRP_RECUPERACION_COBER
GRP_RECUPERACION_COBER2
GRP_RECUPERACION_INGRESO
MOTORA_DM
MOTORA_DM_PT0
NDUP_APERCAB_VAL_PT1
OTROS
PAGOSPROVE_IMP
PERIODOS
STG_APERCAB_BSC
STG_APERCAB_COD_RP
STG_APERCAB_RT
STG_PAGOSPROVEEDORES
STG_PRODUCCION2
STG_PRODUCCION2_
STG_PRODUCCION2_2
STG_PRODUCCION2_2_
STG_RECUPERACIONES
STG_VALUACION
TMP_MOTORA_CALCULO_PT0
TMP_PAGOSPROVEEDORES
VALUACION_PT1
VALUACION_PT_0
;
quit;

Libname &lib_insu. clear;
Libname &lib_resu. clear;
cas session_load terminate;

