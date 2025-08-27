***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Septiembre 1, 2021                                                                       *
* TITULO: Creación de Prep Tables Familia 1 para la regla 1.3                                     *
* OBJETIVO: Póliza rehabilitada con reporte de siniestro 15 días después                          *
* ENTRADAS: COBRANZA, CONTROL_DE_AGENTES, APERTURA_DE_REPORTE, PAGOSPROVEEDORES                   *
* SALIDAS: QCS_PREP_PREV_R1_3_ENGINE, QCS_PREP_PREV_R1_3_DETAIL, QCS_PREP_PREV_R1_3_RESULT  

* Modificación de codigo en regla 1.3, solicitada por Hugo Sánchez solicitada el 26/05/2023. 
Se agrega el campo ESTATUS al detalle. La prep-table que se modifica es:

•	 Work.COBRANZA_T4
•	 Work.COBRANZA_T9
•	 QCS_PREP_PREV_R1_3_DETAIL_pre

Modificación realizada por Jacqueline Madrazo (JMO) el 26/05/2023.                               *
***************************************************************************************************;

%include  
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macro_reexpedidas_reexpedicion.sas';

*------------------------*
* Parametros de sistema  *
*------------------------*;
Options compress=NO;
*--------------------------*
* Asignación de librerias  *
*--------------------------*;
%Let LIBN_ORA=INSUMOS;
%Let LIBN_SAS=RESULTS;

%Let Ins_ora_user     = insumos;
%Let Ins_ora_password = "{SAS002}BE8F4D453B0E1DF041C45724440717E65C923343";
%Let Ins_ora_schema   = insumos;
%Let Ins_ora_path     = "QCSPRD01";

%Let Res_ora_user     = resultados;
%Let Res_ora_password = "{SAS002}0E91B656514964920CEA6BA629EB49470E5F96D6";
%Let Res_ora_schema   = resultados;
%Let Res_ora_path     = "QCSPRD01";

Libname &LIBN_ORA. oracle path=&Ins_ora_path. schema="&Ins_ora_schema." user="&Ins_ora_user." password=&Ins_ora_password.
preserve_tab_names=no preserve_col_names=no readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;

Libname &LIBN_SAS. oracle path=&Res_ora_path. schema="&Res_ora_schema." user="&Res_ora_user." password=&Res_ora_password.
preserve_tab_names=no preserve_col_names=no readbuff=32767 insertbuff=32767
db_length_semantics_byte=no
dbclient_max_bytes=1
dbserver_max_bytes=1;

/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

*--------------------------------------------------*
* Parametro: Rango de Dias de Reporte de Siniestro *
*--------------------------------------------------*;
%LET DIA_REP_INI=0;
%LET DIA_REP_FIN=15;
*-------------------------------------------*
* Obtiene la lista de exclusion de Polizas  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_POLIZAS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_3
	WHERE NAME_OF_THE_RULE='PREVENCION 1.3' AND PARAMETER_NAME='LISTA_POLIZAS';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Agentes  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AGENTE AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_3
	WHERE NAME_OF_THE_RULE='PREVENCION 1.3' AND PARAMETER_NAME='LISTA_AGENTE';
QUIT;
RUN;

data poliza_flotilla;
	set &LIBN_ORA..COBRANZA(keep=POL INCISO);
	length INCISO2 $10.;
	INCISO2=compress(compress(INCISO), "0123456789", 'kis');
	INCISO_num=input(compress(INCISO2, , "kw"), best32.);

	if INCISO_num >  1 then
		do;
			output poliza_flotilla;
		end;
run;

proc  sort data=poliza_flotilla(keep= POL) out=pol_flotilla_unique nodupkey;
   by POL;
quit;

proc sql;
create table COBRANZA_PII as
SELECT DISTINCT 
                  POL, 
                  INCISO, 
                  INCISOS 
              FROM &LIBN_ORA..COBRANZA (keep=POL ENDO INCISO INCISOS AGENTE FEC_EMI)
              WHERE 
					datepart(FEC_EMI) between  &fec_inicio. and &fec_fin. AND
                    ENDO='000000' AND 
                    INCISO='0001' AND 
                    INCISOS= 1 
                 /*Elimina falsos positivos, identificados como poliza individuales */
                    and   POL not in(select POL from  pol_flotilla_unique);      
quit;

proc sort data=COBRANZA_PII nodupkey;
 by POL;
quit;

/*Obtiene datos del agente*/
PROC SQL;
CREATE TABLE CONTROL_DE_AGENTES AS
SELECT DISTINCT
                   CVE_AGENTE AS CVE_AGTE, 
                   AGENTE_NOM AS NOM_AGTE, 
                   CLAVE_UNICA_AGENTE,
                   GERENTE
               FROM &LIBN_ORA..CONTROL_DE_AGENTES;
QUIT;

proc sort data=CONTROL_DE_AGENTES nodupkey; /**/
  by CLAVE_UNICA_AGENTE CVE_AGTE ;
quit;

*------------------------------------------------------------------*
* Obtiene los movimientos de las polizas individuales de cobranza  *
* Integra la informacion del agente unico  (Agente, Gerente)       *
*------------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T1_PRE as
select t2.POL as POLIZA, t2.ENDO, t2.INCISO, t2.INCISOS, t2.ESTATUS_INCISO, t2.SERIE, t2.DESDE, t2.HASTA, t2.FEC_EMI, 
         t2.AGENTE, t2.ASEG, t2.ASEGURADO, t2.PRIMA_TOTAL, t2.PMA_PAGADA, t2.PMA_PENDIENTE, t2.USUARIO_EMI, t2.DESCRIP, 
         t2.FORMA_PAGO, t2.SINIESTROS, t2.USO, t2.NOM_OFIC, t2.REMESA, t2.ESTATUS, t2.RECIBOS, t2.RECIBOS_PAGADOS, 
         t2.SUBRAMO, t2.BATCHDATE 
  /*Identifica las polizas individuales*/
    from COBRANZA_PII t1
  inner join &LIBN_ORA..COBRANZA t2 
    ON (t1.POL=t2.POL);
QUIT;

proc sort data=COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
run;

data COBRANZA_T1_PRE;
set COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
if last.SUBRAMO;
drop SUBRAMO BATCHDATE;
run;

Proc sql; 
create table Work.COBRANZA_T1 as
select T2.*,
         t3.CVE_AGTE, t3.NOM_AGTE, t3.CLAVE_UNICA_AGENTE, t3.GERENTE
FROM COBRANZA_T1_PRE AS t2
  /*Incluye la información de agente unico*/
  left join CONTROL_DE_AGENTES t3 on (t2.AGENTE=t3.CVE_AGTE)
where /*Descarta las polizas contenidas en la lista de exclusion*/
t2.POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
/*Descarta los agentes contenidos en la lista de exclusion*/
and t2.AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE)
/*Descarta los registros con estatus_inciso = F R */
and  upcase(kstrip(t2.estatus_inciso)) ne upcase("F R");
Quit;

/*Inicia proceso de asignación de agente */
/*Asigna a la póliza la clave de agente con endoso =000000
 Regla de cve de agente= si la póliza tiene mas dos claves unica de agente, se tomara para la póliza 
 la clave unica de agente con endoso=000000 
 */
  /*Obtiene la clave unica de agente con endodoso=000000 */
proc sql;    
 create table cve_agente_endo_0 as  
  select poliza,
         endo,
         FEC_EMI,
        CVE_AGTE as CVE_AGTE_2,
        NOM_AGTE as NOM_AGTE_2,
        CLAVE_UNICA_AGENTE as CLAVE_UNICA_AGENTE_2,
        GERENTE as  GERENTE_2
 from  COBRANZA_T1
   where  ENDO='000000';
quit;
  
proc sort data=cve_agente_endo_0;
  by poliza endo descending FEC_EMI;
quit;

proc sort data=cve_agente_endo_0 out=cve_agente_endo_0 nodupkey;
  by poliza ;
quit;
    
proc sql;
 create table COBRANZA_T1_0  as
   select  b.*,
      CVE_AGTE_2,
      NOM_AGTE_2,
      CLAVE_UNICA_AGENTE_2,
      GERENTE_2
  from  COBRANZA_T1 b
   left join  cve_agente_endo_0 c
    on b.poliza=c.poliza;
quit;   
          
data COBRANZA_T1_1 (drop=
   AGENTE_TMP
   CVE_AGTE_TMP 
   NOM_AGTE_TMP
   CLAVE_UNICA_AGENTE_TMP
   GERENTE_TMP
   CVE_AGTE_2
   NOM_AGTE_2
   CLAVE_UNICA_AGENTE_2
   GERENTE_2
  );
 set  COBRANZA_T1_0 (rename= (
         CVE_AGTE=CVE_AGTE_TMP
         NOM_AGTE=NOM_AGTE_TMP 
         CLAVE_UNICA_AGENTE=CLAVE_UNICA_AGENTE_TMP
         GERENTE=GERENTE_TMP
         AGENTE = AGENTE_TMP
   ) );
   length CLAVE_UNICA_AGENTE_ORG $200.;
   length  CVE_AGTE  $5.;
   length  AGENTE   $5.; 
   length  NOM_AGTE   $100.;
   length  CLAVE_UNICA_AGENTE  $200.;
   length  GERENTE $150.;

    CLAVE_UNICA_AGENTE_ORG = CLAVE_UNICA_AGENTE_TMP;
  
   if  missing(kstrip(CLAVE_UNICA_AGENTE_2)) = 0  then do;
     AGENTE = CVE_AGTE_2;
     CVE_AGTE = CVE_AGTE_2;
     NOM_AGTE = NOM_AGTE_2;
     CLAVE_UNICA_AGENTE = CLAVE_UNICA_AGENTE_2;
     GERENTE =GERENTE_2;
   end;  
   else if  missing(kstrip(CLAVE_UNICA_AGENTE_2)) = 1 then do;
     AGENTE = AGENTE_TMP;
     CVE_AGTE = CVE_AGTE_TMP;
     NOM_AGTE = NOM_AGTE_TMP;
     CLAVE_UNICA_AGENTE = CLAVE_UNICA_AGENTE_TMP;
     GERENTE =GERENTE_TMP;
   end;
run;

data  COBRANZA_T1;
  set  COBRANZA_T1_1;
run;

/*Termina proceso de asignación de agente */

*----------------------------------------------------------*
* Mantiene los movimientos de polizas con rehabilitaciones *
*----------------------------------------------------------*;
Data Work.COBRANZA_T2;
 Set Work.COBRANZA_T1;
 if substr(ENDO,1,1)='1' then output;
Run;   
*------------------------------------------------------------------------------*
* Identifica las polizas rehabilitadas con estatus: PAGADA o PENDIENTE DE PAGO *
*------------------------------------------------------------------------------*; 
Data Work.COBRANZA_T3;
 Set Work.COBRANZA_T2;

 length ESTATUS_POLIZA $30.;

 if INCISOS=1 and
    INCISO='0001' and
    substr(strip(ENDO),1,1)='1' and
    find(ESTATUS_INCISO,'0001-VIGENTE','i') ge 1 and
/* Para la regla 1.3 considerar remesa a “0” O “VACIA” (para contemplar ambas opciones) */
      ( missing(kstrip(REMESA))=1 OR kstrip(REMESA) = '0' ) and 
    ESTATUS in ('I',' ') and
    PMA_PENDIENTE > 0 and 
    RECIBOS ne RECIBOS_PAGADOS then
    ESTATUS_POLIZA = 'REHABILITADA PENDIENTE DE PAGO';
 else
 if INCISOS=1 and
    INCISO='0001' and
    substr(strip(ENDO),1,1)='1' and
    find(upcase(ESTATUS_INCISO),'0001-VIGENTE','i') ge 1 and
    countc(REMESA,'9') <= 2 and
    upcase(ESTATUS) in ('I',' ') and
    PMA_PAGADA > 0 and
    PMA_PENDIENTE = 0 and 
    RECIBOS=RECIBOS_PAGADOS then
    ESTATUS_POLIZA = 'REHABILITADA PAGADA';
Run; 
*-----------------------------------------------------------------------------------------------*
* Identifica las polizas siniestradas con fecha de reporte 15 dias despues de su rehabilitacion *
*-----------------------------------------------------------------------------------------------*;
data  pre_apertura_report (drop= BATCHDATE  SYSUSERID); 
  set  &LIBN_ORA..APERTURA_REPORTE;
run;

proc sort data=pre_apertura_report  out=uni_pre_apertura_report  nodupkey;
  by _ALL_;
quit;

Proc sql;
create table Work.COBRANZA_T4 as
select t1.POLIZA, t1.ENDO, t1.INCISO, t1.INCISOS, t1.ESTATUS_INCISO, t1.DESDE, t1.HASTA, t1.FEC_EMI, t1.ASEG, t1.SERIE,
       t1.PMA_PAGADA, t1.PMA_PENDIENTE, t1.USUARIO_EMI, t1.DESCRIP, t1.PRIMA_TOTAL, t1.FORMA_PAGO, t1.USO,
       t1.NOM_OFIC, t1.AGENTE, t1.SINIESTROS, t1.CVE_AGTE, t1.NOM_AGTE, t1.CLAVE_UNICA_AGENTE, t1.GERENTE,
       t2.REPORTE, t2.FEC_HORA_REP, 
	   t1.ESTATUS, /* Inclusión del campo ESTATUS – JMO 26/05/2023 */
       catt(substr(t2.REPORTE,1,4),t2.SINIESTRO) as SINIESTRO length=11, 
       intck('dtday', t1.FEC_EMI, t2.FEC_HORA_REP) as DIAS_REP_SIN,
       t1.ESTATUS_POLIZA
  from Work.COBRANZA_T3 as t1 
  inner join uni_pre_apertura_report as t2 on t1.POLIZA=t2.POLIZA
where calculated DIAS_REP_SIN between &DIA_REP_INI. and &DIA_REP_FIN. and t2.SINIESTRO is not missing; 
Quit;

*--------------------------------------------------------------------------------*
* Obtiene el importe pagado para Gastos, Indemnizaciones y Ordenes Tradicionales *
*--------------------------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T5 as
select distinct t1.NRO_SINIESTRO, t1.CODIGO_DE_PAGO, t1.IMPORTE_PAGO
  from &LIBN_ORA..PAGOSPROVEEDORES as t1 
 inner join Work.COBRANZA_T4 as t2 on t1.NRO_SINIESTRO=t2.SINIESTRO;
Quit;

Proc sql;
create table Work.COBRANZA_T6 as
select NRO_SINIESTRO,
       case when CODIGO_DE_PAGO=' ' then 'ORDEN_TRADICIONAL'
            when CODIGO_DE_PAGO='I' then 'INDEMNIZACIONES'
            when CODIGO_DE_PAGO='G' then 'GASTOS'
            else 'NA' end as CODIGO_DE_PAGO,
       sum(IMPORTE_PAGO) AS IMPORTE_PAGO format=COMMA15.2
from Work.COBRANZA_T5
where CODIGO_DE_PAGO in (' ','I','G')
group by NRO_SINIESTRO, CODIGO_DE_PAGO;
Quit;


%macro ejecuta_transpose;
%let num_obs=0;
proc contents data =Work.COBRANZA_T6 (obs = 1) out = work.num_obs(keep = nobs) noprint; 
run; 
data _null_; 
         set work.num_obs (obs = 1); 
         call symput("num_obs", nobs); 
run;
%put &=num_obs;   

%if  %eval(&num_obs >  0 ) %then %do;
  proc transpose data=Work.COBRANZA_T6 
                out=Work.COBRANZA_T7(drop=_NAME_) 
             prefix=IMPORTE_SIN_;
    id CODIGO_DE_PAGO;
    var IMPORTE_PAGO;
    by NRO_SINIESTRO;
  run;
%end;
%else %do;
proc sql;
create table WORK.COBRANZA_T7
  (
   NRO_SINIESTRO char(11) format=$11. informat=$11. label='NRO_SINIESTRO',
   IMPORTE_SIN_GASTOS num format=COMMA15.2,
   IMPORTE_SIN_INDEMNIZACIONES num format=COMMA15.2,
   IMPORTE_SIN_ORDEN_TRADICIONAL num format=COMMA15.2
  );
quit;
%end;
%mend;

%ejecuta_transpose;

*--------------------------------------------*
* Obtiene el importe total de los siniestros *
*--------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T8 as
select NRO_SINIESTRO, 
       IMPORTE_SIN_INDEMNIZACIONES,
       IMPORTE_SIN_GASTOS,
       IMPORTE_SIN_ORDEN_TRADICIONAL,
       sum(IMPORTE_SIN_INDEMNIZACIONES, 
           IMPORTE_SIN_GASTOS, 
           IMPORTE_SIN_ORDEN_TRADICIONAL) as IMPORTE_TOTAL_SINIESTROS format=COMMA15.2
from Work.COBRANZA_T7;
Quit;
*---------------------------------------------*
* Integra las variables para la Tabla Detalle *
*---------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T9 as
select t1.POLIZA,
       t1.INCISO,
       t1.ESTATUS_INCISO,
       t1.ENDO,
       t1.SERIE,
       t1.FEC_EMI,
       t1.DESDE, 
       t1.HASTA, 
       t1.ASEG, 
       t1.AGENTE, 
       t1.PRIMA_TOTAL, 
       t1.PMA_PAGADA,
       t1.PMA_PENDIENTE,
       t1.USUARIO_EMI,
       t1.DESCRIP, 
       t1.FORMA_PAGO, 
       t1.SINIESTROS,
       t1.USO, 
       t1.NOM_OFIC,
       t1.GERENTE,
       t1.CVE_AGTE, 
       t1.NOM_AGTE,
       t1.CLAVE_UNICA_AGENTE,
       t1.SINIESTRO,
       t1.REPORTE, 
       t1.FEC_HORA_REP,
       t1.DIAS_REP_SIN,
       t1.ESTATUS_POLIZA,
       t1.ESTATUS, /* Inclusión del campo ESTATUS – JMO 26/05/2023 */
       t2.IMPORTE_SIN_INDEMNIZACIONES,
       t2.IMPORTE_SIN_GASTOS,
       t2.IMPORTE_SIN_ORDEN_TRADICIONAL,
       t2.IMPORTE_TOTAL_SINIESTROS
  from Work.COBRANZA_T4 as t1 
  left join Work.COBRANZA_T8 as t2 
     on t1.SINIESTRO=t2.NRO_SINIESTRO;
Quit;

*-------------------------------------------------------*
* Obtiene el total de polizas  por agente unico *
*-------------------------------------------------------*;
Proc sql; 
create table input_COBRANZA_T1 as
select T2.*,
         t3.CVE_AGTE, t3.NOM_AGTE, t3.CLAVE_UNICA_AGENTE, t3.GERENTE
FROM COBRANZA_T1_PRE AS t2
  /*Incluye la información de agente unico*/
 left join CONTROL_DE_AGENTES t3 on (t2.AGENTE=t3.CVE_AGTE)
where  missing(t2.SERIE) = 0  
 /*Descarta las polizas contenidas en la lista de exclusion*/
and t2.POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
/*Descarta los agentes contenidos en la lista de exclusion*/
and t2.AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE);
Quit;

data  input_COBRANZA_T1;
  set input_COBRANZA_T1;
    key_unique = _N_;
    if  kstrip(ENDO)='000000';
run;

%obtiene_reexpedida_reexpedicion(
   tabla_pol_indiv_endo_0=input_COBRANZA_T1, /*Tiene solo movimientos con endoso ='000000' */
   tabla_pol_indiv_all_endo=COBRANZA_T1_PRE,  /*Tiene  todos los movimientos */
   UMBRAL_REXP=0      /*parámetro de umbral */
); 

Proc sql;  
create table WORK.COBRANZA_T10 as
	select
		t1.CLAVE_UNICA_AGENTE,
		count(distinct(t1.POLIZA)) AS CNT_POL_VIG
	from
		WORK.input_COBRANZA_T1 t1
	where
		     kstrip(ENDO) = '000000'  and missing(CLAVE_UNICA_AGENTE) =0
           and  poliza in (select  distinct poliza from _OUT_REEXPEDIDA_REEXPEDICION 
                            where FLG_REEXPEDICION_V = 0     )
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;
*--------------------------------------------------------------------------*
* Obtiene el total de polizas rehabilitadas con siniestro por agente unico *
*--------------------------------------------------------------------------*;
Proc sql;
create table WORK.COBRANZA_T11 as
	select
		t1.CLAVE_UNICA_AGENTE,
		count(distinct(t1.POLIZA)) AS CNT_POL_REH
	from
		WORK.COBRANZA_T9 t1
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;
*----------------------------------------------------------------------------------*
* Obtiene el % promedio de siniestralidad año anterior y en curso por agente unico *
*----------------------------------------------------------------------------------*;

Data WORK.COBRANZA_T12_TMP;
 Set &LIBN_ORA..CONTROL_DE_AGENTES
(keep=CLAVE_UNICA_AGENTE 
      CVE_AGENTE 
      PORC_SIN_MES_ANT
      PORC_SIN_A_DIC_ANIO_ANT 
      PORC_SIN_MES_ANIO_ANT);
Run;
Data WORK.COBRANZA_T12_TMP;
modify WORK.COBRANZA_T12_TMP;
array VARS{*} PORC_SIN_MES_ANT PORC_SIN_A_DIC_ANIO_ANT PORC_SIN_MES_ANIO_ANT;
do i = 1 to dim(VARS);
/*los valores menores o iguales a cero se reemplazan por nulos*/
   if VARS{i} <= 0 then call missing(VARS{i});
end;
Run;
Proc sql;
create table WORK.COBRANZA_T12 as
	select
		t1.CLAVE_UNICA_AGENTE,
        mean(t1.PORC_SIN_MES_ANT) AS PORC_SIN_MES_ANT,
        mean(t1.PORC_SIN_MES_ANIO_ANT) as PORC_SIN_ANIO_CURSO,
		mean(t1.PORC_SIN_A_DIC_ANIO_ANT) AS PORC_SIN_ANIO_ANTERIOR
	from
		WORK.COBRANZA_T12_TMP t1
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;
*-------------------------------------------------*
* Obtiene el total de siniestros por agente unico *
*-------------------------------------------------*;
Proc sql;
create table WORK.COBRANZA_T13 as
	select
		t1.CLAVE_UNICA_AGENTE,
		count(distinct(t1.SINIESTRO)) AS CNT_SINIESTROS
	from
		WORK.COBRANZA_T9 t1
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;
*-------------------------------------------*
* Creacion de la Tabla de Resultado Entidad *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_3_RESULT) by oracle;
disconnect from oracle;
QUIT;
Proc sql;
create table QCS_PREP_PREV_R1_3_RESULT_pre as
	select
		t1.CLAVE_UNICA_AGENTE,
        t4.CNT_SINIESTROS,
		t1.CNT_POL_REH,
        t2.CNT_POL_VIG,
        divide(t1.CNT_POL_REH,t2.CNT_POL_VIG) as PCT_POL_REH_SIN,
        t3.PORC_SIN_ANIO_ANTERIOR, 
        t3.PORC_SIN_ANIO_CURSO,
   	   DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID length=10,
       DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
	from
		WORK.COBRANZA_T11 t1
     /* Obtiene el total de polizas  por agente unico */
	left join WORK.COBRANZA_T10 t2 on
    t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE

    /* Obtiene el % promedio de siniestralidad año anterior y en curso por agente unico */
    left join WORK.COBRANZA_T12 t3 on
    t1.CLAVE_UNICA_AGENTE=t3.CLAVE_UNICA_AGENTE
     /* Obtiene el total de siniestros por agente unico */
    left join WORK.COBRANZA_T13 t4 on
    t1.CLAVE_UNICA_AGENTE=t4.CLAVE_UNICA_AGENTE;
Quit;

proc append data=QCS_PREP_PREV_R1_3_RESULT_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_3_RESULT;
run;

*-----------------------------------------*
* Genera las Prep Tables Motora y Detalle *
*-----------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_3_ENGINE) by oracle;
disconnect from oracle;
QUIT;
Proc sql;
create table QCS_PREP_PREV_R1_3_ENGINE_pre as 
    select POLIZA,
       ENDO,
       INCISOS,
       ESTATUS_INCISO,
       DESDE,
       HASTA,
       FEC_EMI,
       AGENTE,
       SINIESTROS,
       CVE_AGTE,
       NOM_AGTE,
       CLAVE_UNICA_AGENTE,
       REPORTE,
       FEC_HORA_REP,
       SINIESTRO,
       DIAS_REP_SIN,
       ESTATUS_POLIZA,
   	   DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID length=10,
       DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
      from Work.COBRANZA_T4;
Quit;

proc append data=QCS_PREP_PREV_R1_3_ENGINE_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_3_ENGINE;
run;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_3_DETAIL) by oracle;
disconnect from oracle;
QUIT;
Proc sql;
create table QCS_PREP_PREV_R1_3_DETAIL_pre as
    select POLIZA,
       INCISO,
       ESTATUS_INCISO,
       ENDO,
       SERIE,
       FEC_EMI,
       DESDE, 
       HASTA, 
       ASEG, 
       AGENTE, 
       PRIMA_TOTAL, 
       PMA_PAGADA,
       PMA_PENDIENTE,
       USUARIO_EMI,
       DESCRIP, 
       FORMA_PAGO, 
       SINIESTROS,
       USO, 
       NOM_OFIC,
       GERENTE,
       CVE_AGTE, 
       NOM_AGTE,
       CLAVE_UNICA_AGENTE,
       SINIESTRO,
       REPORTE, 
       FEC_HORA_REP,
       DIAS_REP_SIN,
       ESTATUS_POLIZA,
       IMPORTE_SIN_INDEMNIZACIONES,
       IMPORTE_SIN_GASTOS,
       IMPORTE_SIN_ORDEN_TRADICIONAL,
       IMPORTE_TOTAL_SINIESTROS,
       ESTATUS, /* Inclusión del campo ESTATUS – JMO 26/05/2023 */
   	   DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID length=10,
       DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
      from Work.COBRANZA_T9;
Quit;

proc append data=QCS_PREP_PREV_R1_3_DETAIL_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_3_DETAIL force;
run;
 

/*Borra tablas temporales */
proc datasets lib = WORK nolist nowarn memtype = (data view);
   delete  COBRANZA_PII
COBRANZA_T1
COBRANZA_T10
COBRANZA_T11
COBRANZA_T12
COBRANZA_T12_TMP
COBRANZA_T13
COBRANZA_T1_PRE
COBRANZA_T2
COBRANZA_T3
COBRANZA_T4
COBRANZA_T5
COBRANZA_T6
COBRANZA_T7
COBRANZA_T8
COBRANZA_T9
CONTROL_DE_AGENTES
FORMATS
LISTA_AGENTE
LISTA_POLIZAS
NUM_OBS
POLIZA_FLOTILLA
POL_FLOTILLA_UNIQUE
QCS_PREP_PREV_R1_3_DETAIL_PRE
QCS_PREP_PREV_R1_3_ENGINE_PRE
QCS_PREP_PREV_R1_3_RESULT_PRE
input_COBRANZA_T1
_OUT_REEXPEDIDA_REEXPEDICION
;
quit;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;