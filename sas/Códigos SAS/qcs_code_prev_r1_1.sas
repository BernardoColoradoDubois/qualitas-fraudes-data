***************************************************************************************************
* AUTOR: SAS Institute Mexico (smxeba)                                                            *
* FECHA: Enero 10, 2022                                                                           *
* TITULO: Creación de Prep-Table Familia 1 para la regla 1.1                                      *
* OBJETIVO: Determinar carruseles para un mismos agente y sin pago                                *
* ENTRADAS: COBRANZA, CONTROL_DE_AGENTES                                                          *
* SALIDAS: QCS_PREP_PREV_R1_1_ENGINE, QCS_PREP_PREV_R1_1_DETAIL, QCS_PREP_PREV_R1_1_RESULT        *
***************************************************************************************************;

%include 
	'/qcs/projects/default/qcsrun/sas/macros/prevencion/macros_utility_prev.sas';
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

*------------------------------------------------------------------------*
* Parametro: Obtiene los dias definidos para validacion de reexpedicion  *
*------------------------------------------------------------------------*;
PROC SQL NOPRINT;
    SELECT PARAMETER_VALUE
     INTO :UMBRAL_REXP
        FROM &LIBN_SAS..QCS_PARAM_PREV_R1_1
	WHERE NAME_OF_THE_RULE='PREVENCION 1.1' AND PARAMETER_NAME='UMBRAL_REEXP';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Polizas  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_POLIZAS AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_1
	WHERE NAME_OF_THE_RULE='PREVENCION 1.1' AND PARAMETER_NAME='LISTA_POLIZAS';
QUIT;
RUN;
*-------------------------------------------*
* Obtiene la lista de exclusion de Agentes  *
*-------------------------------------------*;
PROC SQL;
	CREATE TABLE WORK.LISTA_AGENTE AS
    SELECT PARAMETER_VALUE FROM &LIBN_SAS..QCS_PARAM_PREV_R1_1
	WHERE NAME_OF_THE_RULE='PREVENCION 1.1' AND PARAMETER_NAME='LISTA_AGENTE';
QUIT;
RUN;
*--------------------------------------------------------------*
* Parametro: Dias definidos para considerar vigencias montadas *
*--------------------------------------------------------------*;

/*Se toma un intervalo de 12 meses a partir de la fecha en curso, manteniendo una historia de 12 meses*/
	%put &=fec_inicio;
	%put &=fec_fin;
	%put &=fec_inicio_ora;
	%put &=fec_fin_ora;
	%put &=fec_borra;
	%put &=fec_borra_ora;

%let DIAS_VIGMON = 15;
*--------------------------------------------------------------------------------------------------------------------------*
* Identifica las polizas individuales de cobranza, filtrando los numeros de SERIE que tenga al menos un valor alfanumerico *
* Integra la informacion del agente unico  (Agente, Gerente)                                                               *
*--------------------------------------------------------------------------------------------------------------------------*;

/*Obtiene pólizas de tipo flotilla*/
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
create table Polizas_individuales as
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
  
proc sort data=Polizas_individuales nodupkey;
   by POL;
quit;

/*Obtiene datos de cobranza*/
proc sql;
  create table cobranza_periodo as 
   select t1.subramo, 
         t1.POL as POLIZA, t1.ENDO, t1.INCISO, t1.INCISOS, t1.ESTATUS_INCISO, t1.SERIE, t1.DESDE, t1.HASTA, t1.FEC_EMI, 
         t1.AGENTE, t1.ASEG, t1.ASEGURADO, t1.PRIMA_TOTAL, t1.PMA_PAGADA, t1.USUARIO_EMI, t1.DESCRIP, t1.FORMA_PAGO, 
         t1.SINIESTROS, t1.USO, t1.NOM_OFIC, t1.BATCHDATE 
        from  &LIBN_ORA..COBRANZA t1
        inner join    Polizas_individuales i
        on t1.POL = i.pol;
quit;

/*Se valida registros duplicados partiendo de la llave  
  subramo, pol, endo e inciso que hace un registro unico en la fuente de cobranza
*/
proc sort data=cobranza_periodo;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
run;

data cobranza_periodo;
set cobranza_periodo;
by POLIZA ENDO INCISO SUBRAMO BATCHDATE;
if last.SUBRAMO;
drop SUBRAMO BATCHDATE;
run;

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
        
proc sql;
 create table COBRANZA_T1 as 
   select c.*
      ,a.CVE_AGTE,a.NOM_AGTE,a.CLAVE_UNICA_AGENTE,a.GERENTE
    from  cobranza_periodo c
      left join  CONTROL_DE_AGENTES a 
       on c.AGENTE=a.CVE_AGTE
    where missing(serie) = 0 
    /*Descarta las polizas contenidas en la lista de exclusion*/
and POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
/*Descarta los agentes contenidos en la lista de exclusion*/
and AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE);
quit;


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

data cobranza_periodo2;
  set  COBRANZA_T1_1;
run;

data  COBRANZA_T1;
  set  COBRANZA_T1_1;
   if  kstrip(ENDO)='000000'; 
run;
/*Termina proceso de asignación de agente */

*--------------------------------------------*
* Identifica al menos tres polizas por SERIE *
*--------------------------------------------*;
Proc sql;
   create table Work.COBRANZA_T2 as
   select t1.* from Work.COBRANZA_T1 as t1
   inner join
        (select SERIE, count(poliza) as count 
         from Work.COBRANZA_T1
         group by SERIE) as t2
   on (t1.SERIE = t2.SERIE)
   Where t2.count >= 3
   Order by t1.SERIE, t1.DESDE, t1.FEC_EMI, t1.POLIZA;
Quit;
    
*----------------- INICIO: <<IDENTIFICA REEXPEDICIONES>> -----------------*;
data input_COBRANZA_T2;
    set  COBRANZA_T2;
  key_unique = _N_;
     if  kstrip(ENDO)='000000' and  missing(serie) = 0  and  anyalpha(serie) > 0 ;
run;
  
%obtiene_reexpedida_reexpedicion(
   tabla_pol_indiv_endo_0=input_COBRANZA_T2, /*Tiene solo movimientos con endoso ='000000' */
   tabla_pol_indiv_all_endo=cobranza_periodo,  /*Tiene  todos los movimientos */
   UMBRAL_REXP=&UMBRAL_REXP.    /*parámetro de umbral */
); 
*----------------- TERMINO: <<IDENTIFICA REEXPEDICIONES>> -----------------*;


*-------------------------------------------------------------------------*
* Integra ultimo movimiento de reexpedicion considerando polizas sin pago *
*-------------------------------------------------------------------------*;  
Data Work.COBRANZA_T15 (where=(ESTATUS_INCISO = '0001-CANCELADO'));
 Set _OUT_REEXPEDIDA_REEXPEDICION (where=(FLG_REEXPEDICION_V = 0));
Run;   

*--------------------------------------------------------------*
* Identifica series con al menos tres pólizas por agente unico *
*--------------------------------------------------------------*;
Proc sql; 
   create table Work.COBRANZA_T16 as
   select t1.*
   from Work.COBRANZA_T15  as t1
      inner join
        (select t1.SERIE, t1.CLAVE_UNICA_AGENTE, count(*) as count
		   from WORK.COBRANZA_T15 t1
		  group by t1.SERIE, t1.CLAVE_UNICA_AGENTE) as t2
      on (t1.SERIE = t2.SERIE and t1.CLAVE_UNICA_AGENTE = t2.CLAVE_UNICA_AGENTE)
   Where t2.count >= 3
   Order by t1.SERIE, t1.DESDE, t1.FEC_EMI, t1.POLIZA;
Quit;


*---------------------------------------------------------------------*
* Identifica las vigencias montadas de cada combinacion SERIE y DESDE *
*---------------------------------------------------------------------*;
Data Work.COBRANZA_T17(drop=count2 rename=(DESDE=DESDE_R HASTA=HASTA_R)) 
     Work.COBRANZA_T18( drop=count1);
 Set Work.COBRANZA_T16;
  count1 + 1;
  count2 + 1;
  by SERIE DESDE FEC_EMI POLIZA;
  /*asigna un consecutivo dependiendo el numero de registros de cada SERIE*/
  if first.SERIE then do; count1 = 1; count2 =2; end;
  /*asigna un consecutivo a partir del siguiente registro de cada SERIE*/
  if last.SERIE then count2 = count2 - 2;
Run;

Proc SQL;
  create table Work.COBRANZA_T19 as
  select t1.*,
         t2.DESDE_R, t2.HASTA_R , t2.count1, t2.poliza as poliza_sig
  from Work.COBRANZA_T18 as t1 left join Work.COBRANZA_T17 as t2
  on (t1.SERIE=t2.SERIE and
      t1.count2=t2.count1) /*relaciona por el consecutivo y la SERIE*/
  order by t1.SERIE, t1.DESDE, t1.FEC_EMI, t1.POLIZA;
Quit;
*--------------------------------*
* Obtiene las vigencias montadas *
*--------------------------------*;
Data Work.COBRANZA_T20;
 Set Work.COBRANZA_T19;
 by SERIE DESDE FEC_EMI;

 /*calculo de vigencia montada*/
 if DESDE_R <= HASTA then VIGENCIA_MONTADA=abs(intck('dtday', HASTA, DESDE_R)); 
    else VIGENCIA_MONTADA=0;
 if last.SERIE then do;
    if DESDE <= HASTA_R then VIGENCIA_MONTADA=abs(intck('dtday', DESDE, HASTA_R)); else VIGENCIA_MONTADA=0;
 end;
Run;

%put &=DIAS_VIGMON;
Data Work.COBRANZA_T21;
 Set Work.COBRANZA_T20;
/*Toma en cuenta sólo las vigencias montadas mayores a 15 días*/
if VIGENCIA_MONTADA > &DIAS_VIGMON. then output;
Run;
*---------------------------------------------*
* Identifica tres o mas movimientos por SERIE *
*---------------------------------------------*;  
Proc sql;
   create table Work.COBRANZA_T22 as
   select t1.* from Work.COBRANZA_T21 as t1
   inner join
        (select SERIE, count(*) as count 
         from Work.COBRANZA_T21
         group by SERIE) as t2
   on (t1.SERIE = t2.SERIE)
   Where t2.count >= 3
   Order by t1.SERIE, t1.DESDE, t1.FEC_EMI, t1.POLIZA;
Quit;

*---------------------------------------------------*
* Obtiene el largo y numero de unidades en carrusel *
*---------------------------------------------------*;   
Proc sql;
   create table Work.COBRANZA_T23 as
   select t1.*, t2.LARGO_CARRUSEL, t2.UNIDADES_CARRUSEL
   from Work.COBRANZA_T22 as t1
        left join
        (select t1.CLAVE_UNICA_AGENTE, t1.SERIE, 
                count(distinct(t1.POLIZA)) as LARGO_CARRUSEL, 
                count(distinct t1.SERIE) as UNIDADES_CARRUSEL
           from work.COBRANZA_T22 t1
          group by t1.CLAVE_UNICA_AGENTE, t1.SERIE) as t2
        on (t1.CLAVE_UNICA_AGENTE = t2.CLAVE_UNICA_AGENTE and
            t1.SERIE=t2.SERIE)
   Order by t1.SERIE, t1.DESDE, t1.FEC_EMI, t1.POLIZA;
Quit;

/*Obtiene todas las pólizas y series montadas*/
data list_all_polizas_montada1(keep=serie poliza  )  list_all_polizas_montada2(keep=serie poliza_sig   rename=(poliza_sig=poliza));  
  set COBRANZA_T23;
  output list_all_polizas_montada1;
  output list_all_polizas_montada2;
run;

proc  append base = list_all_polizas_montada2 data=list_all_polizas_montada1;
quit;

proc sort data=list_all_polizas_montada2 nodupkey;
  by serie poliza ;
quit;

proc sql;
  create table  list_all_polizas_montada2_1 as 
  select distinct  b.serie, b.POLIZA, b.ENDO, b.INCISO, 1 as flg_pol_carrusel from   COBRANZA_T20 b
   inner join  list_all_polizas_montada2 m
   on   b.serie = m.serie
   and  b.poliza = m.poliza;
quit;

proc sql;
  create table  list_all_pol_ctn_carrusel as 
     select distinct  b.serie, b.POLIZA, b.ENDO, b.INCISO, 1 as flg_pol_cnt_carrusel from   COBRANZA_T23 b;
quit;
 
*----------------------------------------------------------------------------------*
* Obtiene el % promedio de siniestralidad año anterior y en curso por agente unico *
*----------------------------------------------------------------------------------*;
Data WORK.CONTROL_DE_AGENTES2;
 Set &LIBN_ORA..CONTROL_DE_AGENTES
(keep=CLAVE_UNICA_AGENTE 
      CVE_AGENTE 
      PORC_SIN_MES_ANT
      PORC_SIN_A_DIC_ANIO_ANT 
      PORC_SIN_MES_ANIO_ANT);
Run;
Data WORK.CONTROL_DE_AGENTES2;
modify WORK.CONTROL_DE_AGENTES2;
array VARS{*} PORC_SIN_MES_ANT PORC_SIN_A_DIC_ANIO_ANT PORC_SIN_MES_ANIO_ANT;
do i = 1 to dim(VARS);
/*los valores menores o iguales a cero se reemplazan por nulos*/
   if VARS{i} <= 0 then call missing(VARS{i});
end;
Run;

Proc sql;
create table WORK.COBRANZA_T24 as
	select
		t1.CLAVE_UNICA_AGENTE,
        mean(t1.PORC_SIN_MES_ANT) AS PORC_SIN_MES_ANT,
        mean(t1.PORC_SIN_MES_ANIO_ANT) as PORC_SIN_ANIO_CURSO,
		mean(t1.PORC_SIN_A_DIC_ANIO_ANT) AS PORC_SIN_ANIO_ANTERIOR
	from
		WORK.CONTROL_DE_AGENTES2 t1
	group by
		t1.CLAVE_UNICA_AGENTE;
Quit;

*------------------------*
* Genera la Tabla Motora *
*------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_1_ENGINE) by oracle;
disconnect from oracle;
QUIT;

Proc sql;
	insert into &LIBN_SAS..QCS_PREP_PREV_R1_1_ENGINE
    select t1.CLAVE_UNICA_AGENTE, t1.CVE_AGTE, t1.NOM_AGTE, t1.SERIE, t1.DESDE, t1.HASTA, t1.FEC_EMI, 
           t1.POLIZA, t1.ENDO, t1.INCISO, t1.INCISOS, t1.ESTATUS_INCISO, t1.ASEG, t1.ASEGURADO, t1.PMA_PAGADA,
           t1.PRIMA_TOTAL, t1.VIGENCIA_MONTADA, t1.LARGO_CARRUSEL, t1.UNIDADES_CARRUSEL, 
           t2.PORC_SIN_ANIO_CURSO, t2.PORC_SIN_ANIO_ANTERIOR,
       	   DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID,
           DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
      from Work.COBRANZA_T23 t1
    left join WORK.COBRANZA_T24 t2 on
    t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE;
Quit;

*-------------------------*
* Genera la Tabla Detalle *
*-------------------------*;
PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_1_DETAIL) by oracle;
disconnect from oracle;
QUIT;
  
PROC SQL;
	create table  tmp_QCS_PREP_PREV_R1_1_DETAIL1 as 
        SELECT t1.GERENTE, t1.CLAVE_UNICA_AGENTE, t1.CVE_AGTE, t1.NOM_AGTE, t1.SERIE, t1.POLIZA, t1.ENDO, t1.INCISO, 
           t1.FEC_EMI, t1.DESDE, t1.HASTA, t1.ASEG, t1.ESTATUS_INCISO, t1.PRIMA_TOTAL, t1.PMA_PAGADA, t1.USUARIO_EMI, 
           t1.DESCRIP, t1.FORMA_PAGO, t1.SINIESTROS, t1.USO, t1.NOM_OFIC,

           case  when  missing(t3.flg_pol_carrusel) ne 1 then  t3.flg_pol_carrusel
           else  0
           end as  flg_pol_carrusel,
           case  when  missing(t4.flg_pol_cnt_carrusel) ne 1 then  t4.flg_pol_cnt_carrusel
           else  0
           end as  flg_pol_cnt_carrusel,

           DATETIME() as BATCHDATE,
           "&SYSUSERID." AS SYSUSERID length=10,
           DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
        FROM 
           (  select 
                     GERENTE , CLAVE_UNICA_AGENTE ,CVE_AGTE ,NOM_AGTE, 
                     AGENTE,
                     SERIE, POLIZA, ENDO, INCISO, FEC_EMI, DESDE, HASTA, ASEG, ESTATUS_INCISO, PRIMA_TOTAL,
                     PMA_PAGADA, USUARIO_EMI, DESCRIP, FORMA_PAGO, SINIESTROS, USO, NOM_OFIC
              from  cobranza_periodo2
           ) AS t1

  /*Incluye la información de agente unico*/
  /*left join  CONTROL_DE_AGENTES  as t2 
   on t1.AGENTE=t2.CVE_AGTE */

 /*Agrega flag de poliza montada*/
  left join list_all_polizas_montada2_1 as t3
   on  t1.serie  = t3.serie
   and t1.poliza = t3.poliza
   and t1.endo   = t3.endo
   and t1.inciso = t3.inciso
 /*Agrega flag de conteo de poliza carrusel*/
  left join   list_all_pol_ctn_carrusel t4
   on  t1.serie  = t4.serie
   and t1.poliza = t4.poliza
   and t1.endo   = t4.endo
   and t1.inciso = t4.inciso
     

WHERE   t1.SERIE IN (SELECT DISTINCT SERIE FROM &LIBN_SAS..QCS_PREP_PREV_R1_1_ENGINE) 
	/*Descarta las polizas contenidas en la lista de exclusion */
	and t1.POLIZA not in (select distinct PARAMETER_VALUE from Work.LISTA_POLIZAS)
	/*Descarta los agentes contenidos en la lista de exclusion */
	and t1.AGENTE not in (select distinct PARAMETER_VALUE from Work.LISTA_AGENTE);
QUIT;

proc sort data=_OUT_REEXPEDIDA_REEXPEDICION (keep =   POLIZA
                 SERIE
                 FLG_REEXPEDICION_V
                 CON_PMA_PAGADA)    out=FLG_REEXPEDICION_V4
 nodupkey;
   by               POLIZA
                    SERIE;
quit; 

  
  /*Marca las polizas que son reexpedidas*/  
proc sql;
 create table  tmp_QCS_PREP_PREV_R1_1_DETAIL2 as
  select  t1.*,
         /* si valor FLG_REEXPEDICION_V= 1 es reexpedida si el valor es 0 es reexpedición
            si el valor FLG_REEXPEDICION_V = . no tiene endoso=000000 por lo cual no se clasifica
          */
          CASE
             WHEN t3.FLG_REEXPEDICION_V =. THEN . ELSE t3.FLG_REEXPEDICION_V 
          END AS FLG_REEXPEDICION
 from   tmp_QCS_PREP_PREV_R1_1_DETAIL1 t1
  left join (select  
                 SERIE,
                 POLIZA,
                 FLG_REEXPEDICION_V
               from  FLG_REEXPEDICION_V4 ) as t3
  on t1.SERIE=t3.SERIE  and 
     t1.POLIZA=t3.POLIZA 

   order by  t1.SERIE, t1.DESDE,t1.FEC_EMI, t1.POLIZA;
quit;

proc append  base=&LIBN_SAS..QCS_PREP_PREV_R1_1_DETAIL data=tmp_QCS_PREP_PREV_R1_1_DETAIL2 force;
quit;

  
*-------------------------------------------------------*
* Obtiene el total de polizas por agente unico *
*-------------------------------------------------------*;
data input_COBRANZA_T1;
    set  COBRANZA_T1;
  key_unique = _N_;
     if  kstrip(ENDO)='000000' and  missing(serie) = 0  ;
run;

%obtiene_reexpedida_reexpedicion(
   tabla_pol_indiv_endo_0=input_COBRANZA_T1, /*Tiene solo movimientos con endoso ='000000' */
   tabla_pol_indiv_all_endo=cobranza_periodo,  /*Tiene  todos los movimientos */
   UMBRAL_REXP=&UMBRAL_REXP.    /*parámetro de umbral */
); 

PROC SQL;       
CREATE TABLE WORK.COBRANZA_T26 AS
	SELECT
		t1.CLAVE_UNICA_AGENTE,
		COUNT(DISTINCT(t1.POLIZA)) AS CNT_POL_VIG
	FROM
		WORK.COBRANZA_T1 t1
  where    
     kstrip(t1.ENDO) = '000000' and missing(t1.CLAVE_UNICA_AGENTE) =0
           and  t1.poliza in (select  distinct poliza from _OUT_REEXPEDIDA_REEXPEDICION 
                            where FLG_REEXPEDICION_V = 0     )
	GROUP BY
		t1.CLAVE_UNICA_AGENTE;
QUIT;
RUN;

*-------------------------------*
* Genera la Tabla de Resultados *
*-------------------------------*;
PROC SQL;
	CREATE TABLE WORK.COBRANZA_T27 AS
		SELECT
			t1.CLAVE_UNICA_AGENTE,
			COUNT(DISTINCT(t1.POLIZA)) AS LARGO_CARRUSEL,
			COUNT(DISTINCT(t1.SERIE)) AS UNIDADES_CARRUSEL,
            MEAN(t1.PORC_SIN_ANIO_CURSO) AS PORC_SIN_ANIO_CURSO, 
            MEAN(t1.PORC_SIN_ANIO_ANTERIOR) AS PORC_SIN_ANIO_ANTERIOR
		FROM
			&LIBN_SAS..QCS_PREP_PREV_R1_1_ENGINE t1
		GROUP BY
			t1.CLAVE_UNICA_AGENTE
    ;
QUIT;
RUN;


PROC SQL;
connect to oracle
(user=&Res_ora_user. password=&Res_ora_password. path=&Res_ora_path.);
execute (truncate table QCS_PREP_PREV_R1_1_RESULT) by oracle;
disconnect from oracle;
QUIT;

PROC SQL;
	INSERT INTO &LIBN_SAS..QCS_PREP_PREV_R1_1_RESULT
		SELECT 
            t1.CLAVE_UNICA_AGENTE,
			t1.LARGO_CARRUSEL,
			t1.UNIDADES_CARRUSEL,
			t2.CNT_POL_VIG,  /*Total de pólizas por agentes si tomar encuenta el estatus */
			DIVIDE(t1.LARGO_CARRUSEL,t2.CNT_POL_VIG) AS PCT_POL_CARRUSEL,
            t1.PORC_SIN_ANIO_CURSO, 
            t1.PORC_SIN_ANIO_ANTERIOR,
            DATETIME() as BATCHDATE,
            "&SYSUSERID." AS SYSUSERID,
            DHMS(&FEC_FIN.,0,0,0) AS PERIOD_BATCHDATE
		FROM
			WORK.COBRANZA_T27 t1
	LEFT JOIN WORK.COBRANZA_T26 t2 
        ON t1.CLAVE_UNICA_AGENTE=t2.CLAVE_UNICA_AGENTE;
QUIT;
RUN; 
 
Libname &LIBN_ORA. clear;
Libname &LIBN_SAS. clear;