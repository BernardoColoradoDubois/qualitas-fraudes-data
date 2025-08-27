*************************************************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                                                              *
* FECHA: Septiembre 3, 2021                                                                                                         *
* TITULO: Creación de Prep-Table Familia 6 para la regla 6.2                                                                        *
* OBJETIVO: Identificar las pólizas o beneficiarios con 2 o más pagos autorizados con el código de texto 28 (responsabilidad de Q)  *
* ENTRADAS: PAGOSPROVEEDORES                                                                                                        *
* SALIDAS: QCS_PREP_PREV_R6_2_ENGINE, QCS_PREP_PREV_R6_2_DETAIL, QCS_PREP_PREV_R6_2_RESULT                                          *
*************************************************************************************************************************************;
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

*----------------------------------------------*
* Obtiene la lista de exclusion de Ajustadores *
*----------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AJUSTADOR AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R6_2
	WHERE kstrip(NAME_OF_THE_RULE)='PREVENCION 6.2' AND kstrip(PARAMETER_NAME)='LISTA_AJUSTADOR'
    and  missing(PARAMETER_VALUE) =0;
;
QUIT;
RUN;

/*Busca el valor en los nombres de ajustador estandarizado*/
proc sql;
 create table LISTA_AJUSTADOR2 as 
select  distinct
    PARAMETER_VALUE
   , kstrip(u.CLAVE_AJUSTADOR) || '_' || kstrip(u.NOMBRE_AJUSTADOR_GR ) as CVE_NOMBRE_AJUSTADOR_GR length=360
  from LISTA_AJUSTADOR l
inner join 
      (
      select CLAVE_AJUSTADOR
            ,NOMBRE_AJUSTADOR_GR
            ,  kstrip(CLAVE_AJUSTADOR) || '_' || kstrip(NOMBRE_AJUSTADOR ) as cve_nom_ajustador_origen  length=360   
            from   &LIBN_SAS..QCS_AGENTE_UNICO ) u
on  compress(upcase(kstrip(l.PARAMETER_VALUE)), , "kw")  =compress(upcase(kstrip(u.cve_nom_ajustador_origen)), , "kw");
quit;


*-------------------------------------------------------------*
* Obtiene la lista de inclusion de Codigos de Responsabilidad *
*-------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_COD_TXT AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R6_2
	WHERE kstrip(NAME_OF_THE_RULE)='PREVENCION 6.2' AND kstrip(PARAMETER_NAME)='LISTA_COD_TXT';
QUIT;
RUN;
*--------------------------------------------------------------*
* Obtiene la lista de exclusion de Beneficiarios Clusterizados *
*--------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_BENEFICIARIO AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R6_2
	WHERE kstrip(NAME_OF_THE_RULE)='PREVENCION 6.2' AND kstrip(PARAMETER_NAME)='LISTA_BENEFICIARIO'
   and missing(PARAMETER_VALUE) = 0 ;
QUIT;
RUN;
     
/*Busca el nombre de GR que le corresponde al beneficiario */ 
proc sql;
 create table LISTA_BENEFICIARIO2 as 
select  distinct
       u.BENEFICIARIO_GR,l.PARAMETER_VALUE
  from LISTA_BENEFICIARIO l
inner join 
      (
     select  distinct  BENEFICIARIO_GR,BENEFICIARIO   
             from &LIBN_SAS..PAG_PROVEE_BENEF_UNICO    ) u
         
on  compress(upcase(kstrip(l.PARAMETER_VALUE)), , "kw")  =compress(upcase(kstrip(u.BENEFICIARIO)), , "kw");
quit;



*-------------------------------------------*
* Obtiene el umbral para el Numero de Pagos *
*-------------------------------------------*;
PROC SQL NOPRINT;
    SELECT PARAMETER_VALUE INTO :UMBRAL_NUM_PAGOS FROM &LIBN_SAS..QCS_PARAM_PREV_R6_2
	WHERE kstrip(NAME_OF_THE_RULE)='PREVENCION 6.2' AND kstrip(PARAMETER_NAME)='UMBRAL_NUM_PAGOS';
QUIT;
%put &=UMBRAL_NUM_PAGOS;

*---------------------------------------------------------------------*
* Obtiene los ultimos 12 meses de polizas de responsabilidad Qualitas *
*---------------------------------------------------------------------*;
PROC SQL noprint;
connect to oracle as Insumos
(user=&Ins_ora_user. password=&Ins_ora_password. path=&Ins_ora_path.);
select FECHA_PAGO into:MAX_FECHA_PAGO
from connection to Insumos
(select max(FECHA_PAGO) as FECHA_PAGO from PAGOSPROVEEDORES);
disconnect from Insumos;
QUIT;

%put  &=MAX_FECHA_PAGO;


%LET COMMA=%STR(%');
DATA _NULL_;
CALL SYMPUT("DOCE_MESES_ANTES",CATS("&COMMA.",PUT(%SYSFUNC(INTNX(DTMONTH,"&MAX_FECHA_PAGO."dt,-11,B)),DATETIME20.),"&COMMA."));
RUN;
%PUT &=DOCE_MESES_ANTES;

PROC SQL;
connect to oracle as Insumos
(user=&Ins_ora_user. password=&Ins_ora_password. path=&Ins_ora_path.);
create table WORK.tmp_pagos_proveedores as
 select * from connection to Insumos
(select FECHA_PAGO,  
        COD_TXT, 
        CVE_AJU, 
        AJUSTADOR,
		POLIZA,
        NRO_SINIESTRO,
		BENEFICIARIO,
        COBERTURA,
        IMPORTE_PAGO 
   from PAGOSPROVEEDORES
 where FECHA_PAGO >= to_date(&DOCE_MESES_ANTES.,'DDMONYYYY:HH24:MI:SS'))
where /*Incluye los codigos de responsabilidad de la lista de parametros*/
COD_TXT in (select distinct PARAMETER_VALUE from Work.LISTA_COD_TXT);
disconnect from Insumos;
QUIT;

data tmp_pagos_proveedores;
   set tmp_pagos_proveedores;
 key_unique=_N_;
 if  missing(CVE_AJU) = 0 and  missing(AJUSTADOR) = 0;
run;
*------------------------------------------------------------*
* Integra el campo estandarizado de Beneficiario y Ajustador *
*------------------------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.tmp_pagos_proveedores2 AS
		SELECT
        p.key_unique,
		p.FECHA_PAGO,  
        p.COD_TXT, 
        p.AJUSTADOR length=300,
        p.CVE_AJU,
		p.POLIZA,
        p.NRO_SINIESTRO,
        p.BENEFICIARIO,
        p.COBERTURA,
        p.IMPORTE_PAGO ,
  /*Agrega el GR de Agente */
   case
          when missing(u.NOMBRE_AJUSTADOR_GR) = 0 then  kstrip(u.CLAVE_AJUSTADOR) || '_' || kstrip(u.NOMBRE_AJUSTADOR_GR)
          else kstrip(p.CVE_AJU)  || '_'  || kstrip(p.AJUSTADOR) 
       end  as  CVE_NOMBRE_AJUSTADOR_GR length=300,
    /*Agrega el GR de BENEFICIARIO */
  case
          when missing(u.BENEFICIARIO_GR) = 0 then u.BENEFICIARIO_GR
          else  p.BENEFICIARIO
        end  as  BENEFICIARIO_GR length=300

  from  tmp_pagos_proveedores p

    /*Agrega el GR de Agente */
   left join  ( select distinct CLAVE_AJUSTADOR, NOMBRE_AJUSTADOR_GR  
                from &LIBN_SAS..QCS_AGENTE_UNICO ) u
  on   compress(upcase(kstrip(p.CVE_AJU)), , "kw") 
    =  compress(upcase(kstrip(u.CLAVE_AJUSTADOR)), , "kw")

    /*Agrega el GR de BENEFICIARIO */
    LEFT JOIN ( 
        select  distinct  BENEFICIARIO_GR,BENEFICIARIO   
             from &LIBN_SAS..PAG_PROVEE_BENEF_UNICO ) u
     on compress(upcase(kstrip(p.BENEFICIARIO)), , "kw") =compress(upcase(kstrip(u.BENEFICIARIO)), , "kw");
quit;

proc sort data=tmp_pagos_proveedores2 nodupkey;
  by key_unique;
quit;

PROC SQL;
	CREATE TABLE WORK.PAGOS_T1 AS
      select  
        p.FECHA_PAGO,  
        p.COD_TXT, 
        p.AJUSTADOR,
        p.CVE_AJU,
		p.POLIZA,
        p.NRO_SINIESTRO,
        p.BENEFICIARIO,
        p.COBERTURA,
        p.IMPORTE_PAGO ,
        p.CVE_NOMBRE_AJUSTADOR_GR,
        p.BENEFICIARIO_GR
      from  tmp_pagos_proveedores2 p
 where  p.BENEFICIARIO_GR not in (select distinct  BENEFICIARIO_GR from LISTA_BENEFICIARIO2 )
 and    p.CVE_NOMBRE_AJUSTADOR_GR not in (select distinct CVE_NOMBRE_AJUSTADOR_GR from LISTA_AJUSTADOR2);
quit;

*----------------------------------------------------------------------*
* Identifica los ajustadores con mas de 2 pagos en un mes a una poliza *
*----------------------------------------------------------------------*;
PROC SQL;  
	CREATE TABLE WORK.PAGOS_T2 AS
		SELECT
			t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.BENEFICIARIO_GR,
			COUNT(DISTINCT t1.NRO_SINIESTRO) AS NUM_PAGOS
		FROM
			WORK.PAGOS_T1 t1
		GROUP BY
			t1.CVE_NOMBRE_AJUSTADOR_GR,
            t1.BENEFICIARIO_GR;
QUIT;

data PAGOS_T2;
    set PAGOS_T2;
    if NUM_PAGOS &UMBRAL_NUM_PAGOS.;
run;

proc sort data=PAGOS_T2 nodupkey;
  by CVE_NOMBRE_AJUSTADOR_GR  BENEFICIARIO_GR;
quit;


PROC SQL;
	CREATE TABLE WORK.PAGOS_T3 AS
		SELECT t1.*, t2.NUM_PAGOS
		FROM
			WORK.PAGOS_T1 t1
		INNER JOIN WORK.PAGOS_T2 t2
		ON t1.CVE_NOMBRE_AJUSTADOR_GR=t2.CVE_NOMBRE_AJUSTADOR_GR AND
           t1.BENEFICIARIO_GR=t2.BENEFICIARIO_GR;
QUIT;
RUN;
*-----------------------------------------------------*
* Genera las Prep Tables Motora, Detalle y Resultados *
*-----------------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R6_2_ENGINE) by oracle;
disconnect from oracle;
QUIT;
PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_2_ENGINE
    SELECT
       CVE_NOMBRE_AJUSTADOR_GR,
       FECHA_PAGO,
       COD_TXT,
       AJUSTADOR,
       POLIZA,
       NRO_SINIESTRO,
       BENEFICIARIO_GR,
       BENEFICIARIO,
       DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID,
       DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
    FROM Work.PAGOS_T1;
QUIT;
RUN;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_2_DETAIL 
         	where trunc(PERIOD_BATCHDATE)=TRUNC(TO_DATE(&fec_fin_ora, 'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_2_DETAIL
    SELECT
       CVE_NOMBRE_AJUSTADOR_GR,
       NRO_SINIESTRO,
       COD_TXT,
       COBERTURA,
       IMPORTE_PAGO,
	   AJUSTADOR,
       POLIZA,
       BENEFICIARIO_GR,
       BENEFICIARIO,
       FECHA_PAGO,
       NUM_PAGOS,
       DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID,
       DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
    FROM WORK.PAGOS_T3;
QUIT;
RUN;
*-------------------------------------------*
* Mantiene historia de 12 meses en la tabla *
*-------------------------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (delete from QCS_PREP_PREV_R6_2_DETAIL 
    where trunc(PERIOD_BATCHDATE) <=TRUNC(TO_DATE(&fec_borra_ora,'YYYY-MM-DD HH24:MI:SS'))
) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R6_2_RESULT) by oracle;
disconnect from oracle;
QUIT;
PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R6_2_RESULT
    SELECT 
       CVE_NOMBRE_AJUSTADOR_GR,
       FECHA_PAGO,
       COD_TXT,
       AJUSTADOR,
       POLIZA,
       NRO_SINIESTRO,
       BENEFICIARIO_GR,
       BENEFICIARIO, 
       NUM_PAGOS,
       DATETIME() as BATCHDATE,
       "&SYSUSERID." AS SYSUSERID,
       DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
    FROM WORK.PAGOS_T3;
QUIT;
RUN;

Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;