******************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                               *
* FECHA: Septiembre 3, 2021                                                                          *
* TITULO: Creación de Prep-Table Familia 1 para regla 1.6                                            *
* OBJETIVO: Pólizas de servicio público emitidas en estado de circulación diferente                  *
* ENTRADAS: COBRANZA, CONTROL_DE_AGENTES, APERTURA_REPORTE, PAGOS_PROVEEDORES                        *
* SALIDAS: QCS_PREP_PREV_R1_6_ENGINE, QCS_PREP_PREV_R1_6_DETAIL, QCS_PREP_PREV_R1_6_RESULT           *
******************************************************************************************************;

%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macro_reexpedidas_reexpedicion.sas';
*------------------------*
* Parametros de sistema  *
*------------------------*;
Options compress=YES;
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
*-------------------------------------------*
* Obtiene la lista de exclusion de Polizas  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_POLIZAS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_6
	WHERE NAME_OF_THE_RULE='PREVENCION 1.6' AND PARAMETER_NAME='LISTA_POLIZAS';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Agentes  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AGENTE AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_6
	WHERE NAME_OF_THE_RULE='PREVENCION 1.6' AND PARAMETER_NAME='LISTA_AGENTE';
QUIT;
RUN;
*--------------------------*
* Obtiene la lista de Usos *
*--------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_USOS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_6
	WHERE NAME_OF_THE_RULE='PREVENCION 1.6' AND PARAMETER_NAME='LISTA_USOS';
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

proc sort data=CONTROL_DE_AGENTES nodupkey;
  by CLAVE_UNICA_AGENTE CVE_AGTE ;
quit;

*-------------------------------------------------------------*
* Identifica las polizas individuales de cobranza             *
* Integra la informacion del agente unico  (Agente, Gerente)  *
*-------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T1_PRE as
 select t2.POL as POLIZA, t2.ENDO, t2.INCISO, t2.INCISOS, t2.SERIE, t2.DESDE, t2.HASTA, t2.FEC_EMI, t2.REMESA, t2.ESTATUS_INCISO,
         t2.AGENTE, t2.USO, t2.ESTADO, t2.ASEG, t2.ASEGURADO, t2.PRIMA_TOTAL, t2.PMA_PAGADA, t2.PMA_PENDIENTE, 
         t2.USUARIO_EMI, t2.DESCRIP, t2.FORMA_PAGO, t2.SINIESTROS, t2.NOM_OFIC, t2.CP, t2.DIR_ASEG_CALLE, 
         t2.SUBRAMO, t2.BATCHDATE
    FROM COBRANZA_PII t1
  /*Obtiene las polizas individuales*/
  INNER JOIN &LIBN_ORA..COBRANZA t2 ON t1.POL=t2.POL;
quit;

proc sort data=COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
run;

data COBRANZA_T1_PRE;
set COBRANZA_T1_PRE;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
if last.SUBRAMO;
drop SUBRAMO BATCHDATE;
run;
  
/**************/
Proc sql;
create table Work.COBRANZA_T1 as
 SELECT t2.*, 
         t3.CVE_AGTE, t3.NOM_AGTE, t3.CLAVE_UNICA_AGENTE, t3.GERENTE
from COBRANZA_T1_PRE t2
  /*Incluye la información de agente unico*/
  left join CONTROL_DE_AGENTES t3 on t2.AGENTE=t3.CVE_AGTE
where /*Descarta las polizas contenidas en la lista de exclusion*/
POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
/*Descarta los agentes contenidos en la lista de exclusion*/
and AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE)
;
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


*-------------------------------------------------------------*
* Filtra por USO para identificar polizas de servicio publico *
*-------------------------------------------------------------*;
Proc sql;
   create table Work.COBRANZA_T2 as
   select * from Work.COBRANZA_T1 
	where kstrip(substr(USO,1,2)) in (select distinct kstrip(PARAMETER_VALUE) from Work.LISTA_USOS)
order by POLIZA, DESDE, FEC_EMI;
Quit;

*-------------------------------------------*
* Obtiene el ultimo movimiento de la poliza *
*-------------------------------------------*;
Data Work.COBRANZA_T3;
 Set COBRANZA_T2;
  by POLIZA DESDE FEC_EMI;
     if last.POLIZA then output;
Run;

/*Valida duplicados de tabla APERTURA_REPORTE */
data  pre_apertura_report (drop= BATCHDATE  SYSUSERID); 
  set  &LIBN_ORA..APERTURA_REPORTE;
run;

proc sort data=pre_apertura_report  out=uni_pre_apertura_report  nodupkey;
  by _ALL_;
quit;

*--------------------------------------------------------------*
* Identifica los reportes y siniestros asociados a las polizas *
*--------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T4 as
select t2.CLAVE_UNICA_AGENTE, t1.POLIZA, t1.REPORTE, 
       case when t1.SINIESTRO is not missing then
            compress(substr(t1.REPORTE,1,4)||t1.SINIESTRO) 
            else t1.SINIESTRO end as SINIESTRO length=11, 
       t1.ENTIDAD, t2.ESTADO
  from uni_pre_apertura_report (keep=POLIZA REPORTE SINIESTRO ENTIDAD) as t1 
  inner join Work.COBRANZA_T3 as t2 on t1.POLIZA=t2.POLIZA
where /*Descarta las polizas contenidas en la lista de exclusion*/
t1.POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
order by t1.POLIZA;
Quit;
*---------------------------------------------------*
* Obtiene el catalogo de SEPOMEX a nivel de entidad *
*---------------------------------------------------*;
Proc sql;
create table Work.COD_ESTADOS as 
select distinct C_ESTADO, 
	   case 
           when upcase(strip(D_ESTADO)) = 'COAHUILA DE ZARAGOZA' then 'COAHUILA'
      	   when upcase(strip(D_ESTADO)) in ('CIUDAD DE M??XICO', 'CIUDAD DE MÉXICO') then 'CIUDAD DE MEXICO'
           when upcase(strip(D_ESTADO)) in ('M??XICO', 'MÉXICO') then 'ESTADO DE MEXICO'
           when upcase(strip(D_ESTADO)) in ('MICHOAC??N DE OCAMPO', 'MICHOACÁN DE OCAMPO') then 'MICHOACAN'
           when upcase(strip(D_ESTADO)) in ('NUEVO LE??N', 'NUEVO LEÓN') then 'NUEVO LEON'
           when upcase(strip(D_ESTADO)) in ('QUER??TARO', 'QUERÉTARO') then 'QUERETARO'
           when upcase(strip(D_ESTADO)) in ('SAN LUIS POTOS??', 'SAN LUIS POTOSÍ') then 'SAN LUIS POTOSI'
           when upcase(strip(D_ESTADO)) = 'VERACRUZ DE IGNACIO DE LA LLAVE' then 'VERACRUZ'
           when upcase(strip(D_ESTADO)) in ('YUCAT??N', 'YUCATÁN') then 'YUCATAN'
       else upcase(D_ESTADO) 
       end as D_ENTIDAD 
  from &LIBN_ORA..COD_ESTADOS
where input(C_ESTADO,8.) between 1 and 32
order by C_ESTADO;
Quit;
*--------------------------------------------------------------------------*
* Identifica si el reporte se presento en diferente estado de la republica *
*--------------------------------------------------------------------------*;
Proc sql;
create table Work.COBRANZA_T5 as
select t1.*, t2.D_ENTIDAD,
case when t2.D_ENTIDAD = t1.ESTADO then 'SI' else 'NO' end as MISMO_ESTADO
from COBRANZA_T4 t1 
left join Work.COD_ESTADOS t2 on substr(left(t1.ENTIDAD),1,2)=t2.C_ESTADO
order by t1.POLIZA;
Quit;
*-------------------------------------------------------------------------------*
* Obtiene el importe pagado por Gastos, Indemnizaciones y Ordenes Tradicionales *
*-------------------------------------------------------------------------------*;
Proc sql ;
create table Work.COBRANZA_T6 as
select t1.POLIZA, t1.NRO_SINIESTRO, sum(t1.IMPORTE_PAGO) as TOTAL_PAGO_SINIESTROS format=COMMA15.2
  from &LIBN_ORA..PAGOSPROVEEDORES (keep=POLIZA NRO_SINIESTRO IMPORTE_PAGO CODIGO_DE_PAGO) as t1 
 inner join Work.COBRANZA_T5 as t2 
 on t1.POLIZA=t2.POLIZA and t1.NRO_SINIESTRO=t2.SINIESTRO
where t1.CODIGO_DE_PAGO in (' ','I','G') and
/*Descarta las polizas contenidas en la lista de exclusion*/
t1.POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
group by t1.POLIZA, t1.NRO_SINIESTRO;
Quit;
*--------------------------------------------------------------------------------*
* Obtiene el total de siniestros diferentes a la entidad de emision de la poliza *
*--------------------------------------------------------------------------------*;
Proc sql;
create table WORK.COBRANZA_T7 as
	select
		t1.POLIZA, t1.SINIESTRO,
		count(distinct(t1.SINIESTRO)) AS TOT_SINIESTROS_DIF_EMI
	from
		WORK.COBRANZA_T5 t1
	where
		T1.MISMO_ESTADO = 'NO'
	group by
		t1.POLIZA, t1.SINIESTRO;
Quit;
*------------------------------------------------------------------------------*
* Obtiene el total de reportes diferentes a la entidad de emision de la poliza *
*------------------------------------------------------------------------------*;
Proc sql;
create table WORK.COBRANZA_T8 as
	select
		t1.POLIZA, t1.REPORTE,
		count(distinct(t1.REPORTE)) AS TOT_REPORTES_DIF_EMI
	from
		WORK.COBRANZA_T5 t1
	where
		T1.MISMO_ESTADO = 'NO'
	group by
		t1.POLIZA, t1.REPORTE;
Quit;
*----------------------------------------------------------*
* Obtiene el total de reportes de las polizas siniestradas *
*----------------------------------------------------------*;
Proc sql;
create table WORK.COBRANZA_T9 as
	select
		t1.POLIZA, t1.REPORTE,
        count(distinct(t1.REPORTE)) AS TOTAL_REPORTES
	from
		WORK.COBRANZA_T5 t1
	group by
		t1.POLIZA, t1.REPORTE;
Quit;
*-----------------------------------------*
* Genera las Prep Tables Motora y Detalle *
*-----------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_6_ENGINE) by oracle;
disconnect from oracle;
QUIT;
Proc sql;
create table QCS_PREP_PREV_R1_6_ENGINE_pre as
    select t1.POLIZA,
           t2.ENDO,
           t2.INCISOS,
           t2.DESDE,
           t2.HASTA,
           t2.FEC_EMI,
           t2.AGENTE,
           t2.USO,
           t1.ESTADO,
           t1.REPORTE,
           t1.SINIESTRO,
           t1.ENTIDAD,
           t1.D_ENTIDAD,
           t2.CVE_AGTE, 
           t2.NOM_AGTE, 
           t2.CLAVE_UNICA_AGENTE,
           t1.MISMO_ESTADO,
       	   DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID length=10,
           DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
      from Work.COBRANZA_T5 t1
   left join Work.COBRANZA_T3 t2
   on t1.POLIZA=t2.POLIZA;
Quit;

proc append data=QCS_PREP_PREV_R1_6_ENGINE_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_6_ENGINE;
run;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_6_DETAIL) by oracle;
disconnect from oracle;
QUIT;
Proc sql;
create table QCS_PREP_PREV_R1_6_DETAIL_pre as
    select t1.POLIZA,
           t2.INCISO,
           t2.ENDO,
           t2.SERIE,
           t2.FEC_EMI,
           t2.DESDE,
           t2.HASTA,
           t2.ASEGURADO,
           t2.AGENTE,
           t2.PRIMA_TOTAL,
           t2.PMA_PAGADA,
           t2.PMA_PENDIENTE,
           t2.USUARIO_EMI, 
           t2.DESCRIP, 
           t2.FORMA_PAGO, 
           t2.SINIESTROS, 
           t2.NOM_OFIC, 
           t2.CP, 
           t2.DIR_ASEG_CALLE,
           t2.GERENTE,
           t2.USO,
           t2.CVE_AGTE, 
           t2.NOM_AGTE, 
           t2.CLAVE_UNICA_AGENTE, 
           t1.REPORTE,
           t1.SINIESTRO,
           t1.ENTIDAD,
           t2.ESTADO AS EDO_EMISION,
           t1.D_ENTIDAD AS EDO_SINIESTRO, 
           t3.TOTAL_PAGO_SINIESTROS,
           t5.TOT_SINIESTROS_DIF_EMI,
           t4.TOTAL_REPORTES,
           t6.TOT_REPORTES_DIF_EMI,
       	   DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID length=10,
           DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
      from Work.COBRANZA_T5 t1
   left join Work.COBRANZA_T3 t2
   on t1.POLIZA=t2.POLIZA
   left join Work.COBRANZA_T6 t3
   on t1.POLIZA=t3.POLIZA and t1.SINIESTRO=t3.NRO_SINIESTRO
   left join Work.COBRANZA_T9 t4
   on t1.POLIZA=t4.POLIZA and t1.REPORTE=t4.REPORTE
   left join Work.COBRANZA_T7 t5
   on t1.POLIZA=t5.POLIZA and t1.SINIESTRO=t5.SINIESTRO
   left join Work.COBRANZA_T8 t6
   on t1.POLIZA=t6.POLIZA and t1.REPORTE=t6.REPORTE
where t1.MISMO_ESTADO='NO';
Quit;

proc append data=QCS_PREP_PREV_R1_6_DETAIL_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_6_DETAIL;
run;

*-------------------------------------------------------*
* Obtiene el total de polizas vigentes por agente unico *
*-------------------------------------------------------*;

*----------------- INICIO: <<IDENTIFICA REEXPEDICIONES>> -----------------*;
data input_COBRANZA_T1;
    set  COBRANZA_T1;
  key_unique = _N_;
     if  kstrip(endo)='000000' and  missing(serie) = 0  ;
run;

data input_COBRANZA_T1_PRE;
   set COBRANZA_T1_PRE;
run;
  
%obtiene_reexpedida_reexpedicion(
   tabla_pol_indiv_endo_0=input_COBRANZA_T1, /*Tiene solo movimientos con endoso ='000000' */
   tabla_pol_indiv_all_endo=input_COBRANZA_T1_PRE,  /*Tiene  todos los movimientos */
   UMBRAL_REXP=0      /*parámetro de umbral, el valor de cero indica que traera todas las pólizas con reexpedición */
); 
*----------------- TERMINA: <<IDENTIFICA REEXPEDICIONES>> -----------------*;
  
PROC SQL;
CREATE TABLE WORK.COBRANZA_T10 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS CNT_POL_VIG
	FROM
		WORK.COBRANZA_T1 t1
	WHERE t1.ENDO ='000000' and missing(t1.CLAVE_UNICA_AGENTE ) =0 and   missing(serie) = 0
    and  t1.poliza in (select  distinct poliza from _OUT_REEXPEDIDA_REEXPEDICION 
                            where FLG_REEXPEDICION_V = 0     )
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
RUN;

*---------------------------------------------------------------------------------*
* Obtiene el total de polizas diferentes a la entidad de emision por agente unico *
* diferentes a la entidad de emision de la poliza                                 *
*---------------------------------------------------------------------------------*;
Proc sql;
	CREATE TABLE Work.COBRANZA_T11 AS
		SELECT
			t1.CLAVE_UNICA_AGENTE,
			COUNT(DISTINCT(t1.POLIZA)) AS CNT_POL_DIF_EDO_EMI
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R1_6_DETAIL t1
		GROUP BY
			t1.CLAVE_UNICA_AGENTE;
Quit;
*------------------------------------------------*
* Obtiene el total de reportes por agente unico  *
* diferentes a la entidad de emision de la poliza*
*-----------------------------------------------*;
Proc sql;
create table WORK.COBRANZA_T12 as
	select
		t1.CLAVE_UNICA_AGENTE,
        count(distinct(t1.REPORTE)) AS TOTAL_REPORTES
	from
		&LIBN_SAS..QCS_PREP_PREV_R1_6_DETAIL t1
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;
*-------------------------------------------*
* Creacion de la Tabla de Resultado Entidad *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_6_RESULT) by oracle;
disconnect from oracle;
QUIT;
Proc sql;
create table QCS_PREP_PREV_R1_6_RESULT_pre as
	select
		t1.CLAVE_UNICA_AGENTE,
        t1.CNT_POL_DIF_EDO_EMI,
        t3.TOTAL_REPORTES,
        t2.CNT_POL_VIG,  /*varaible contiene todas las polizas del agente, sin tomar encuenta el estatus */  
        divide(t1.CNT_POL_DIF_EDO_EMI,t2.CNT_POL_VIG) as PCT_POL_DIF_EDO_EMI,
   	   DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID length=10,
       DHMS(&fec_fin.,0,0,0) AS PERIOD_BATCHDATE
	from
		WORK.COBRANZA_T11 t1
	left join WORK.COBRANZA_T10 t2 on
    t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE
	left join WORK.COBRANZA_T12 t3 on
    t1.CLAVE_UNICA_AGENTE=t3.CLAVE_UNICA_AGENTE;
Quit;

proc append data=QCS_PREP_PREV_R1_6_RESULT_pre base=&LIBN_SAS..QCS_PREP_PREV_R1_6_RESULT;
run;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;