BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_PROVEEDORES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

-- Tabla DM_PROVEEDORES
CREATE TABLE INSUMOS.DM_PROVEEDORES (
    ID VARCHAR2(50) PRIMARY KEY,
    NOMBRE VARCHAR2(255),
    MARCA VARCHAR2(100),
    TIPO VARCHAR2(100),
    OFICINA VARCHAR2(100),
    SUSPENCION VARCHAR2(100),
    STATUS VARCHAR2(50),
    FECHA_BAJA DATE,
    CODIGO_ESTADO VARCHAR2(50),
    POBLACION VARCHAR2(100),
    DIRECCION VARCHAR2(255),
    DIRECCION_COMER VARCHAR2(255),
    COLONIA_COMER VARCHAR2(100),
    POBLACION_COMER VARCHAR2(100),
    ESTADO_COMER VARCHAR2(100),
    NUMERO_DOCTO VARCHAR2(100),
    NOMBRE_COMER VARCHAR2(255),
    GRUPO VARCHAR2(100),
    FECHA_ALTA DATE,
    CTA_ESP VARCHAR2(100),
    TIPO_AJUSTE VARCHAR2(100),
    TIPO_MARCA VARCHAR2(100),
    RFC VARCHAR2(50),
    CORREO_COMERCIAL VARCHAR2(255),
    CORREO_CONTACTO VARCHAR2(255),
    CORREO_COMERCIAL_2 VARCHAR2(255),
    MARCA_MOTOS VARCHAR2(100),
    TELEFONO_SIICA VARCHAR2(50),
    AE NUMBER(19),
    LATITUD VARCHAR2(50),
    LONGITUD VARCHAR2(50),
    RECIBE_24 VARCHAR2(10),
    ZONA_PROVEEDOR VARCHAR2(100),
    VED VARCHAR2(50),
    TIPO_PERSONA VARCHAR2(50),
    CEXP NUMBER(19),
    TBODY VARCHAR2(255),
    NUMERO_EMPLEADO VARCHAR2(50)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_CAUSAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

-- Tabla DM_CAUSAS
CREATE TABLE INSUMOS.DM_CAUSAS (
    ID VARCHAR2(50) PRIMARY KEY,
    CAUSA VARCHAR2(255),
    CAUSA_HOMOLOGADA VARCHAR2(255),
    FECHA_LOTE TIMESTAMP,
    ID_USUARIO_SISTEMA VARCHAR2(50)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_OFICINAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

-- Tabla DM_OFICINAS
CREATE TABLE INSUMOS.DM_OFICINAS (
    ID VARCHAR2(50) PRIMARY KEY,
    OFICINA VARCHAR2(100),
    DIRECCION VARCHAR2(255),
    COLONIA VARCHAR2(100),
    POBLACION VARCHAR2(100),
    CODIGO_POBLACION VARCHAR2(50),
    CODIGO_POSTAL VARCHAR2(20),
    ZONA VARCHAR2(100)
);



BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_SINIESTROS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

-- Tabla DM_SINIESTROS (con referencias a tablas ya definidas)
CREATE TABLE INSUMOS.DM_SINIESTROS (
    ID VARCHAR2(100) PRIMARY KEY,
    REPORTE VARCHAR2(100),
    POLIZA VARCHAR2(50),
    ENDOSO VARCHAR2(50),
    INCISO VARCHAR2(20),
    ID_POLIZA_ENDOSO VARCHAR2(100),
    ID_POLIZA_ENDOSO_INCISO VARCHAR2(150),
    ASEGURADO VARCHAR2(200),
    ID_ASEGURADO VARCHAR2(50),
    ID_CAUSA VARCHAR2(50),
    FECHA_OCURRIDO DATE,
    FECHA_REPORTE DATE,
    FECHA_REGISTRO DATE,
    FECHA_INICIO_VIGENCIA DATE,
    MARCA VARCHAR2(100),
    MODELO VARCHAR2(100),
    SERIE VARCHAR2(50),
    SEVERIDAD NUMBER,
    STATUS VARCHAR2(20),
    RESERVA NUMBER(15,2),
    UBICACION VARCHAR2(200),
    ENTIDAD VARCHAR2(100),
    ID_OFICINA VARCHAR2(50),
    LEIDO_SAS VARCHAR2(1),
    ID_OFICINA_DE_ATENCION VARCHAR2(50),
    ID_AJUSTADOR VARCHAR2(50),
    LATITUD VARCHAR2(30),
    LONGITUD VARCHAR2(30),
    CONDUCTOR VARCHAR2(200),
    CODIGO_USO VARCHAR2(20),
    USO VARCHAR2(100),
    SUMA_ASEGURADA NUMBER(15,2),
    PLACA VARCHAR2(20),
    FECHA_EMISION DATE,
    POLIZA_REHABILITADA NUMBER(1),
    FECHA_FIN_VIGENCIA DATE,
    OBSERVACIONES VARCHAR2(4000),
    FECHA_LOTE DATE,
    ID_USUARIO_SISTEMA VARCHAR2(50)
);



BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_ETIQUETA_SINIESTRO CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

-- Tabla DM_ETIQUETA_SINIESTRO (con referencias a DM_SINIESTROS)
CREATE TABLE INSUMOS.DM_ETIQUETA_SINIESTRO (
    ID VARCHAR2(50) PRIMARY KEY,
    ID_REPORTE VARCHAR2(50),
    ID_SINIESTRO VARCHAR2(50),
    CODIGO_ESTATUS VARCHAR2(50),
    FECHA_INVESTIGACION TIMESTAMP,
    TEXTO_INVESTIGACION VARCHAR2(4000),
    USUARIO_INVESTIGACION VARCHAR2(255),
    TEXTO_LOCALIZADO VARCHAR2(4000),
    FECHA_LOCALIZADO TIMESTAMP,
    FECHA_CARGA TIMESTAMP,
    FECHA_LOTE TIMESTAMP,
    ID_USUARIO_SISTEMA VARCHAR2(50),
    CONSECUTIVO NUMBER(19),
    VALIDO_DESDE TIMESTAMP,
    VALIDO_HASTA TIMESTAMP,
    VALIDO NUMBER(1)
);



BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_PAGOS_POLIZAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_PAGOS_POLIZAS (
    ID VARCHAR2(50) PRIMARY KEY,
    FECHA_VENCIMIENTO DATE,
    POLIZA VARCHAR2(50),
    ENDOSO VARCHAR2(50),
    POLIZA_ENDOSO VARCHAR2(100),
    IMPORTE NUMBER,
    REMESA VARCHAR2(50),
    MARCA_IMP DATE,
    MARCA_ANUL DATE,
    PRIMA_NETA NUMBER,
    DERECHO_POLIZA NUMBER,
    RECIBO_FIN NUMBER,
    IVA NUMBER,
    BONIFICACION NUMBER,
    BONIFICACION_RF NUMBER,
    BONIFICACION_TEC NUMBER,
    PRIMA_TOTAL NUMBER,
    USUARIO_APLICA VARCHAR2(50),
    FECHA_APLICACION DATE,
    RFC_FACTURACION VARCHAR2(256),
    USUARIO_EMITE VARCHAR2(50),
    STATUS VARCHAR2(10),
    FECHA_PROCESO DATE,
    FECHA_CARGA TIMESTAMP
);


BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_PAGOS_PROVEEDORES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_PAGOS_PROVEEDORES (
    ID VARCHAR2(4000) PRIMARY KEY,
    ID_TRAMITE VARCHAR2(4000),
    IMPORTE_MANO_OBRA NUMBER,
    IMPORTE_REFACCIONES NUMBER,
    ID_PROVEEDOR VARCHAR2(4000),
    PROVEEDOR VARCHAR2(4000),
    CLAVE_DOCUMENTO VARCHAR2(4000),
    DOCUMENTO VARCHAR2(4000),
    IMPORTE NUMBER,
    ORDEN_PAGO VARCHAR2(4000),
    CODIGO_PAGO VARCHAR2(4000),
    CODIGO_GASTO VARCHAR2(4000),
    FECHA_PAGO TIMESTAMP,
    ID_MONEDA VARCHAR2(4000),
    COBERTURA VARCHAR2(4000),
    POLIZA VARCHAR2(4000),
    ENDOSO VARCHAR2(4000),
    ID_POLIZA_ENDOSO VARCHAR2(4000),
    ID_POLIZA_ENDOSO_INCISO VARCHAR2(4000),
    INCISO VARCHAR2(4000),
    CLAVE_SUBRAMO VARCHAR2(4000),
    TIPO_VEHICULO VARCHAR2(4000),
    USO_VEHICULO VARCHAR2(4000),
    SERVICIO_VEHICULO VARCHAR2(4000),
    AMIS VARCHAR2(4000),
    UNIDAD_VEHICULO VARCHAR2(4000),
    MODELO_VEHICULO VARCHAR2(4000),
    PRIMA_POLIZA NUMBER,
    ASEGURADO VARCHAR2(4000),
    ID_AGENTE VARCHAR2(4000),
    ID_OFICINA_EMISION VARCHAR2(4000),
    ID_OFICINA_ATENCION VARCHAR2(4000),
    ID_SINIESTRO VARCHAR2(4000),
    FECHA_OCURRIDO TIMESTAMP,
    DIAS NUMBER,
    ID_AJUSTADOR VARCHAR2(4000),
    ID_CAUSA VARCHAR2(4000),
    FECHA_REGISTRO TIMESTAMP,
    CODIGO_TEXTO VARCHAR2(4000),
    MARCA_VEHICULO VARCHAR2(4000),
    IMPORTE_DEDUCIBLE NUMBER,
    IMPORTE_TOTAL NUMBER,
    ORIGEN VARCHAR2(4000),
    BANCO VARCHAR2(4000),
    CHEQUE VARCHAR2(4000),
    CARATULA VARCHAR2(4000),
    ENTIDAD VARCHAR2(4000),
    NUMERO_NAGS NUMBER,
    FACTURA VARCHAR2(4000),
    ID_USUARIO_FILTRO VARCHAR2(4000),
    ID_USUARIO_TECNICO VARCHAR2(4000),
    ID_USUARIO_PAGO VARCHAR2(4000),
    ID_USUARIO_RESERVA VARCHAR2(4000),
    ID_VALUACION VARCHAR2(4000),
    TIEMPO_PAGO NUMBER
);



BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_COBERTURAS_MOVIMIENTOS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_COBERTURAS_MOVIMIENTOS (
    ID VARCHAR2(4000) PRIMARY KEY,
    RAMO VARCHAR2(4000),
    SUBRAMO VARCHAR2(4000),
    SINIESTRO VARCHAR2(4000),
    TIPO_SINIESTRO VARCHAR2(4000),
    ID_OFICINA_VENTAS VARCHAR2(4000),
    GERENTE VARCHAR2(4000),
    AGENTE VARCHAR2(4000),
    ID_OFICINA_DE_ATENCION VARCHAR2(4000),
    AJUSTADOR VARCHAR2(4000),
    FECHA_REGISTRO TIMESTAMP,
    FECHA_MOVIMIENTO TIMESTAMP,
    TIPO_MOVIMIENTO VARCHAR2(4000),
    GASTO VARCHAR2(4000),
    TIPO_MONEDA VARCHAR2(4000),
    COBERTURA VARCHAR2(4000),
    IMPORTE NUMBER,
    CAPUFE NUMBER,
    CODIGO_COBERTURA VARCHAR2(4000),
    CODIGO_TEXTO VARCHAR2(4000),
    TIPO_CAMBIO NUMBER,
    REPORTE VARCHAR2(4000),
    USUARIO VARCHAR2(4000),
    POLIZA VARCHAR2(4000),
    RELEVANTES VARCHAR2(4000),
    FECHA_OCURRIDO TIMESTAMP,
    TIPO_VEHICULO VARCHAR2(4000),
    MODELO_VEHICULO VARCHAR2(4000),
    CSTROS NUMBER,
    CRVA_INICIAL NUMBER,
    IRVA_INICIAL NUMBER,
    CAJMAS NUMBER,
    IAJMAS NUMBER,
    CAJMENOS NUMBER,
    IAJMENOS NUMBER,
    CPAGOS NUMBER,
    IPAGOS NUMBER,
    CGASTO NUMBER,
    IGASTO NUMBER,
    CDED NUMBER,
    IREC NUMBER,
    CSALV NUMBER,
    ISALV NUMBER,
    FLAG_REC VARCHAR2(4000),
    COD_RESP VARCHAR2(4000),
    DEPRES1 VARCHAR2(4000),
    DEPRES2 VARCHAR2(4000),
    DEPRES3 VARCHAR2(4000),
    DEPRESLIT VARCHAR2(4000),
    MONTO_INDEM_RECUP NUMBER,
    MONTO_INDEM_RECUP_DM NUMBER,
    IMPORTE_MN NUMBER,
    IRVA_INICIAL_MN NUMBER,
    IAJMASMN NUMBER,
    IAJMENOSMN NUMBER,
    IPAGOSMN NUMBER,
    IGASTOMN NUMBER,
    IDEDMN NUMBER,
    IRECMN NUMBER,
    ISALVMN NUMBER,
    MARCA_RVA VARCHAR2(4000),
    COAS NUMBER,
    NOMBRE_ASEGURADORA VARCHAR2(4000),
    CODIGO_POBLACION_ACC VARCHAR2(4000),
    REC_CANTO NUMBER,
    INVSALV VARCHAR2(4000),
    CODIGO_ESTADO_ACC VARCHAR2(4000),
    SINIESDIA NUMBER,
    CLAVE_ABOGADO VARCHAR2(4000)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_ANALISTAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_ANALISTAS (
    ID VARCHAR2(4000) PRIMARY KEY,
    ANALISTA VARCHAR2(4000),
    NOMBRE VARCHAR2(4000),
    CORREO VARCHAR2(4000),
    ESTATUS VARCHAR2(4000),
    CARRUSEL NUMBER,
    ID_USER_94 VARCHAR2(4000),
    FECHA_LOTE TIMESTAMP,
    ID_USUARIO_SISTEMA VARCHAR2(4000)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_REGISTRO CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;


CREATE TABLE INSUMOS.DM_REGISTRO (
    ID_SINIESTRO VARCHAR2(4000),
    ID_OFICINA VARCHAR2(4000),
    DESCRIPCION_OFICINA VARCHAR2(4000),
    USUARIO VARCHAR2(4000),
    NOMBRE VARCHAR2(4000),
    CORREO VARCHAR2(4000),
    MOTIVO VARCHAR2(4000),
    OBSERVACIONES VARCHAR2(4000),
    DOCUMENTO_ADICIONAL VARCHAR2(4000),
    SINIESTRO_ALERTADO VARCHAR2(4000),
    ID_ANALISTA VARCHAR2(4000),
    FECHA_ASIGNACION TIMESTAMP,
    AREA VARCHAR2(4000),
    PUESTO VARCHAR2(4000),
    ID_MOTIVO VARCHAR2(4000),
    MARCA VARCHAR2(4000),
    MONTO_ESTIMADO NUMBER,
    FECHA_LOTE TIMESTAMP,
    ID_USUARIO_SISTEMA VARCHAR2(4000)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_POLIZAS_VIGENTES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_POLIZAS_VIGENTES (
    ID VARCHAR2(4000) PRIMARY KEY,
    RAMO VARCHAR2(4000),
    POLIZA VARCHAR2(4000),
    ENDOSO VARCHAR2(4000),
    POLIZA_ENDOSO VARCHAR2(4000),
    ID_ASEGURADO VARCHAR2(4000),
    FECHA_EMISION TIMESTAMP,
    VIGENCIA_DESDE TIMESTAMP,
    VIGENCIA_HASTA TIMESTAMP,
    RENOVADA_POR VARCHAR2(4000),
    RENUEVA_A VARCHAR2(4000),
    CODIGO_MONEDA VARCHAR2(4000),
    CODIGO_FACTURACION VARCHAR2(4000),
    CODIGO_EMISION VARCHAR2(4000),
    PORC_RF NUMBER,
    PORC_IVA NUMBER,
    PORC_BONIF NUMBER,
    COD_CAMBIO_DP VARCHAR2(4000),
    RSR VARCHAR2(4000),
    AAMM_TARIFA NUMBER,
    AAMM_VALORES NUMBER,
    AAMM_DERPOL NUMBER,
    SUMA_ASEGURADA NUMBER,
    PRIMA_NETA NUMBER,
    DER_POL NUMBER,
    REC_FIN NUMBER,
    IVA NUMBER,
    BONIFICACION NUMBER,
    BONIF_RF NUMBER,
    BONIF_TEC NUMBER,
    PMA_ENDO_RF NUMBER,
    PRIMA_TOTAL NUMBER,
    CODIGO_PLAN_PAGO VARCHAR2(4000),
    CODIGO_PRODUCTO VARCHAR2(4000),
    AGENTE VARCHAR2(4000),
    PROMOTOR VARCHAR2(4000),
    COMIS_NORMAL NUMBER,
    COMIS_RF NUMBER,
    COMIS_TEC NUMBER,
    COTIZACION NUMBER,
    ENDOSO_QUE_MODIFICA VARCHAR2(4000),
    CODIGO_CONDUCTO_PAGO VARCHAR2(4000),
    ACREEDOR_PREND VARCHAR2(4000),
    MARCA_CANC_AUT VARCHAR2(4000),
    COD_COBZA_COAS VARCHAR2(4000),
    FECHA_AMORTIZACION TIMESTAMP,
    FECHA_REHABILITACION TIMESTAMP,
    ID_OFICINA VARCHAR2(4000),
    FECHA_PROCESO_SISE TIMESTAMP,
    HORA_INICIO_CARGA VARCHAR2(4000),
    HORA_FIN_CARGA VARCHAR2(4000),
    CODIGO_OPERACION VARCHAR2(4000),
    POL_END_COAS VARCHAR2(4000),
    COMPANIA_PILOTO VARCHAR2(4000),
    CIAS_PARTICIPANTES VARCHAR2(4000),
    PART_CIAS VARCHAR2(4000),
    PART_CIAS_PN VARCHAR2(4000),
    CODIGO_CANCELACION VARCHAR2(4000),
    REASEGURO_AUT VARCHAR2(4000),
    TURISTA VARCHAR2(4000),
    CAMBIO_MONEDA NUMBER,
    USUARIO_EMITE VARCHAR2(4000),
    IVA_RCP NUMBER,
    BON_NORM_RCP NUMBER,
    REMESA_DERECHO VARCHAR2(4000),
    AUT_BONIF VARCHAR2(4000),
    AUT_TARIFA_MANUAL VARCHAR2(4000),
    DESCTO_X_ZONA_AUTOS VARCHAR2(4000),
    DESCTO_X_ZONA_CAMIONES VARCHAR2(4000),
    PRORRATEO_DERECHOS VARCHAR2(4000),
    AUT_PRORR_DERECHO VARCHAR2(4000),
    AUT_EE_Y_ADAP VARCHAR2(4000),
    AUT_TAR_EE VARCHAR2(4000),
    RENOVACION_AUTOMATICA VARCHAR2(4000),
    FLOT_POL_IND VARCHAR2(4000),
    AUT_BONIF_RENT_AUT VARCHAR2(4000),
    CONDICIONES_VIGENTES VARCHAR2(4000),
    AUT_RECARGO VARCHAR2(4000),
    AUT_REHAB_MAS60 VARCHAR2(4000),
    AUT_ENDO_POL_VENC VARCHAR2(4000),
    FORMATO_RECIBO VARCHAR2(4000),
    AUT_TARIFA_ESP VARCHAR2(4000),
    DIAS_PER_GRACIA_AGTE NUMBER,
    PORC_DESC_PER_GRACIA NUMBER,
    AUT_POL_TURISTA VARCHAR2(4000),
    NUM_VEH_TUR VARCHAR2(4000),
    PMA_ASIST_TURISTA NUMBER,
    POLITICA_BONO_AGENTE VARCHAR2(4000),
    POLITICA_BONO_PROM VARCHAR2(4000),
    AUT_MODIF_BONO VARCHAR2(4000),
    AUT_REUTILIZAR_COTIZACION VARCHAR2(4000),
    AUT_CAMBIAR_ASEG_ENDO VARCHAR2(4000),
    MARCA_RC_EXTRANJERO VARCHAR2(4000),
    COD_AGENCIA1 VARCHAR2(4000),
    DED_ADMV VARCHAR2(4000),
    AUT_PT_RCBOS VARCHAR2(4000),
    AUT_POL_CON_DED_ADMVO VARCHAR2(4000),
    AUT_PAGO_MENS_TRIM_BUSES VARCHAR2(4000),
    AUT_ELIM_ASISTSAT VARCHAR2(4000),
    PLAN_PISO VARCHAR2(4000),
    AUT_TAR_MANUAL VARCHAR2(4000),
    COD_AGENCIA2 VARCHAR2(4000),
    UDI_AGENCIA2 VARCHAR2(4000),
    AUT_TARIF_DIF_COTIZACION VARCHAR2(4000),
    DER_POL_1ER_RCBO NUMBER,
    USUARIO_MARCA_REC_CANC VARCHAR2(4000),
    AUT_MOD_COBER_EE VARCHAR2(4000),
    AUT_CAMBIO_DP VARCHAR2(4000),
    AUT_RENOVACION VARCHAR2(4000),
    AUT_AJUSTE_AUTOM VARCHAR2(4000),
    POL_COMP_AGENCIAS NUMBER,
    AUT_MOD_COM_NORM VARCHAR2(4000),
    AUT_MOD_COM_X_RECARGO VARCHAR2(4000),
    AUT_CAMBIO_COD_ASEGURADO VARCHAR2(4000),
    AUT_EMITIR_PAQ_FAM VARCHAR2(4000),
    AUT_AMPARAR_SERIES_DUP_C_AGTES VARCHAR2(4000),
    AUT_EMITIR_VEH_FRONT VARCHAR2(4000),
    NUM_ECOLOGICO_OPL VARCHAR2(4000),
    LEYENDA_DED_ADMVO VARCHAR2(4000),
    AUT_EMITIR_MOD_ANT_CON_TAR_NO_PERM VARCHAR2(4000),
    FECHA_CANCELACION_AUT TIMESTAMP,
    AUT_EMITIR_TAR_NO_VIG VARCHAR2(4000),
    EXCLUSION_DIA_BISIESTO VARCHAR2(4000),
    VALIDACION_VEH_NO_ROBADO VARCHAR2(4000),
    AUT_EMITIR_SERVPUB_VIG_183DIA VARCHAR2(4000),
    AUT_AMP_VEH_MENOR25_ANIOS_RCPAS VARCHAR2(4000),
    VIG_GARANTIA_FABRICANTE VARCHAR2(4000),
    AUT_EMITIR_ASEG_MARCADO_ART140 VARCHAR2(4000),
    COD_AGENCIA3 VARCHAR2(4000),
    UDI_AGENCIA3 VARCHAR2(4000),
    SUBRAMO_DESCUENTO VARCHAR2(4000),
    POLIZA_A_MSI VARCHAR2(4000),
    FEC_CREACION_ENDO_X_CFP TIMESTAMP,
    AUT_EMITIR_SERIES_CON_PT VARCHAR2(4000),
    NUMERO_INCISOS_POLIZA NUMBER,
    SA_VEH_NO_INCL_BONIF NUMBER,
    AUT_DED_ENCONTRACK VARCHAR2(4000),
    CP_ASEGURADO VARCHAR2(4000),
    CP_ALTERNO VARCHAR2(4000),
    USUARIO_MODIFICA_CP VARCHAR2(4000),
    COD_EDO_Y_GPO_CP VARCHAR2(4000),
    TIPO_POLIZA VARCHAR2(4000),
    CTRL_DIAS_PER_GRACIA VARCHAR2(4000),
    AUT_DIAS_PER_GRACIA VARCHAR2(4000),
    AUT_MODIF_CP VARCHAR2(4000),
    INCLUYE_ROBO_VALOR_TOTAL VARCHAR2(4000),
    MARCA_EMISION_EN_SISE VARCHAR2(4000),
    AUT_EMITIR_CON_PEPS VARCHAR2(4000),
    IMPRESION_CERT_RC_OBL VARCHAR2(4000),
    COBR_EMITIDAS_EN_INCISOS VARCHAR2(4000),
    AUT_CANCELAR_POL_RC_OBL VARCHAR2(4000),
    COB_AFECTADAS_POR_DED_ADMVO VARCHAR2(4000),
    MARCA_POL_CARGA_OPL VARCHAR2(4000),
    FEC_CARGA_POL_OPL VARCHAR2(4000),
    PMA_NETA_X_INCISO NUMBER,
    PMA_SIN_BT_X_INCISO NUMBER,
    PMA_SIN_ACUMULADO_X_INCISO NUMBER,
    PMA_NETA_BT_X_INCI NUMBER,
    SUBRAMO_X_INCI VARCHAR2(4000),
    DERPOL_X_INCI NUMBER,
    REC_FIN_X_INCISO NUMBER,
    IVA_X_INCI NUMBER,
    TOTAL_X_INCI NUMBER,
    BT_X_INCI NUMBER,
    BN_X_INCI NUMBER,
    BR_X_INCI NUMBER,
    METODO_PAGO_OPL VARCHAR2(4000),
    SW_VERIFICA_AGT VARCHAR2(4000),
    MARCA_AGTE_CUES_DIRECTA VARCHAR2(4000),
    AUT_SERIE_APP VARCHAR2(4000),
    MARCA_POL_DEPURADA_HIST VARCHAR2(4000),
    TIPO_NEGOCIO VARCHAR2(4000),
    HISTORIAL_ADEG_EN_PLIZA VARCHAR2(4000),
    AUT_EMITIR_PERSONA_SUJETA_A_REVISION VARCHAR2(4000),
    AUT_EMITIR_PERSONA_NO_ASEG VARCHAR2(4000),
    BLOQUEO_POLIZAS VARCHAR2(4000),
    POL_EMITIDA_VIA_COBZA_DELEGADA VARCHAR2(4000),
    POLIZA_A_REEXPEDIR VARCHAR2(4000),
    RFC_NORM_ASEG_UNICO VARCHAR2(4000),
    MARCA_PMA_INSUFICIENTE VARCHAR2(4000),
    ENDOSO_CFP VARCHAR2(4000),
    MARCA_CAMBIO_FP_SIN_RF VARCHAR2(4000),
    MARCA_POL_RC_CONTRACTUAL VARCHAR2(4000),
    POL_REFER_RC_CONTRAT VARCHAR2(4000),
    ENDO_REFER_RC_CONTRACT VARCHAR2(4000),
    INCI_REFER_RC_CONTRACT VARCHAR2(4000),
    FEC_EMI_REFER_RC_CONTRACT VARCHAR2(4000),
    DESDE_REFER_RC_CONTRACT VARCHAR2(4000),
    HASTA_REFER_RC_CONTRACT VARCHAR2(4000),
    TIPO_POL_RC_CONTRACT VARCHAR2(4000),
    TIPO_DOC_RC_CONTRACT VARCHAR2(4000),
    RCBOS_SUBSEC_BON_TEC_X_INCI VARCHAR2(4000),
    RCBOS_SUBSEC_BON_TEC_X_INCI_2 VARCHAR2(4000),
    SUBTOTAL NUMBER,
    ASEG_UNICO VARCHAR2(4000),
    AUT_EMITIR_ASEG_ALTO_RIESGO VARCHAR2(4000),
    AUT_SERIES_CANCELADAS VARCHAR2(4000),
    FECHA_PROCESO TIMESTAMP,
    MARCA_POL_AUTOEXPEDIBLE VARCHAR2(4000),
    PORC_BONRF VARCHAR2(4000),
    BM VARCHAR2(4000),
    DESC_PER_GRACIA VARCHAR2(4000),
    TIPO_ENDOSO VARCHAR2(4000),
    CONSECUTIVO NUMBER,
    ULTIMO_ENDOSO NUMBER(1),
    FECHA_CARGA TIMESTAMP
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_ASEGURADOS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_ASEGURADOS (
    ID VARCHAR2(4000) PRIMARY KEY,
    NOMBRE VARCHAR2(4000),
    GIRO VARCHAR2(4000),
    NACIONALIDAD VARCHAR2(4000),
    FOLIO VARCHAR2(4000),
    APODERADO VARCHAR2(4000),
    NACIMIENTO_APODERADO VARCHAR2(4000),
    RFC VARCHAR2(4000),
    ART_140 VARCHAR2(4000),
    FECHA_NACIMIENTO DATE,
    FECHA_ALTA DATE,
    TIPO_ASEGURADOS VARCHAR2(4000),
    CORREO VARCHAR2(4000),
    CURP VARCHAR2(4000),
    ID_OFICINA VARCHAR2(4000),
    NOMBRE_OFICINA VARCHAR2(4000)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_ESTADOS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_ESTADOS (
    ID VARCHAR2(4000) PRIMARY KEY,
    ESTADO_POBLACION VARCHAR2(4000),
    ESTATUS VARCHAR2(4000),
    CLAVE_INEGI VARCHAR2(4000),
    REGION VARCHAR2(4000),
    RADIO_PERFIL VARCHAR2(4000),
    ESTADO_ESTANDAR VARCHAR2(4000),
    ESTADO_POBLACION_2 VARCHAR2(4000)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_TIPOS_PROVEEDORES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_TIPOS_PROVEEDORES (
    ID VARCHAR2(4000) PRIMARY KEY,
    TIPO_PROVEEDOR VARCHAR2(4000),
    GRUPO VARCHAR2(4000),
    NOMBRE_GRUPO VARCHAR2(4000)
);

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_INCISOS_POLIZAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_INCISOS_POLIZAS (
    ID VARCHAR2(4000) PRIMARY KEY,
    SECCION VARCHAR2(4000),
    POLIZA VARCHAR2(4000),
    ENDOSO VARCHAR2(4000),
    ANEXO_INCISO VARCHAR2(4000),
    POLIZA_ENDOSO VARCHAR2(4000),
    MARCA_COMP VARCHAR2(4000),
    MARCA_CORTA VARCHAR2(4000),
    AMIS VARCHAR2(4000),
    SUBRAMO VARCHAR2(4000),
    MODELO VARCHAR2(4000),
    ORIGEN VARCHAR2(4000),
    TIPO_VEH VARCHAR2(4000),
    CARROCERIA VARCHAR2(4000),
    USO VARCHAR2(4000),
    SERVICIO VARCHAR2(4000),
    OCUPANTES NUMBER,
    TONELADAS NUMBER,
    CARGA VARCHAR2(4000),
    TRAILES VARCHAR2(4000),
    URBANO VARCHAR2(4000),
    PLACAS VARCHAR2(4000),
    MOTOR VARCHAR2(4000),
    SERIE VARCHAR2(4000),
    COLOR VARCHAR2(4000),
    TRANSMISION VARCHAR2(4000),
    CILINDROS NUMBER,
    PUERTAS NUMBER,
    ANEXOS VARCHAR2(4000),
    ESTATUS VARCHAR2(4000),
    TIPO_COBER VARCHAR2(4000),
    CATEGORIA VARCHAR2(4000),
    TARIF_AUT VARCHAR2(4000),
    VALOR_0KM NUMBER,
    SUMA_ASEGURADA NUMBER,
    SUMA_ASEGURADA_GM NUMBER,
    SUMA_ASEGURADA_RC NUMBER,
    SUMA_ASEGURADA_ADAP NUMBER,
    NUMERO_PASAJEROS NUMBER,
    PRIMA_TOTAL NUMBER,
    PRIMA_DM NUMBER,
    PRIMA_RT NUMBER,
    PRIMA_RC NUMBER,
    PRIMA_GM NUMBER,
    PRIMA_EE NUMBER,
    PRIMA_ADAP NUMBER,
    PRIMA_RT_ADAP NUMBER,
    CALC_PRORRATA VARCHAR2(4000),
    DEDUCIBLE_DM NUMBER,
    DEDUCIBLE_RT NUMBER,
    DEDUCIBLE_RC NUMBER,
    DEDUCIBLE_EE NUMBER,
    DEDUCIBLE_DM_ADAP NUMBER,
    DEDUCIBLE_RT_ADAP NUMBER,
    REC_RC_TRAILER NUMBER,
    REC_RC_X_DANO_CARGA NUMBER,
    DESC_DM_X_USO NUMBER,
    DESC_RT_X_USO NUMBER,
    DESC_RC_X_USO NUMBER,
    DESC_GM_X_USO NUMBER,
    NOMBRE_CONDUCTOR VARCHAR2(4000),
    SUMA_ASEG_ACC_OCUP NUMBER,
    SUMA_ASEG_EXT_RC NUMBER,
    SUMA_ASEG_GL NUMBER,
    PRIMA_RCP NUMBER,
    PRIMA_EXT_RC NUMBER,
    PRIMA_ACC_OCUP NUMBER,
    PRIMA_GT NUMBER,
    PRIMA_GL NUMBER,
    PRIMA_SPT NUMBER,
    PRIMA_EXE_DED NUMBER,
    PAQUETE_GL NUMBER,
    DESC_RP NUMBER,
    SUMA_ASEG_RP NUMBER,
    DED_RP NUMBER,
    PMA_RP NUMBER,
    PMA_AJUSTE_INFL NUMBER,
    SUMA_ASEG_AV NUMBER,
    TASA_AV NUMBER,
    PRIMA_EXT_TERRITORIAL NUMBER,
    PRIMA_RC_ECOLOGICA NUMBER,
    SUMA_ASEG_RC_ECOLOGICA NUMBER,
    DIAS_DED_RC_ECOLOGICA NUMBER,
    DESC_DED_RC_ECOLOGICA NUMBER,
    PRIMA_RC_LEGAL NUMBER,
    SUMA_ASEG_RC_LEGAL NUMBER,
    DIAS_DED_RC_LEGAL NUMBER,
    DESC_DED_RC_LEGAL NUMBER,
    SUMA_ASEG_RCB NUMBER,
    PRIMA_RCB NUMBER,
    DIAS_DED_RCB NUMBER,
    SUMA_ASEG_RCP NUMBER,
    DIAS_DED_RCP NUMBER,
    SUMA_ASEG_RCC NUMBER,
    PRIMA_RCC NUMBER,
    DIAS_DED_RCC NUMBER,
    DESC_DED_RCB NUMBER,
    DESC_DED_RCP NUMBER,
    DESC_DED_RCC NUMBER,
    PAQUETE_AV NUMBER,
    PRIMA_SERV_ASISTENCIA NUMBER,
    DESCRIP_CARGA VARCHAR2(4000),
    REPUVE VARCHAR2(4000),
    DED_RC_PASAJERO NUMBER,
    PRIMA_DANO_X_CARGA NUMBER,
    PRIMA_ASISTENCIA_SATELITAL NUMBER,
    UNIDAD_DE_SALVAMENTO VARCHAR2(4000),
    SERIE_ENCONTRACK VARCHAR2(4000),
    FEC_INST_ENCONTRACK DATE,
    SUMA_ASEG_RC_EXT NUMBER,
    PRIMA_RC_EXT NUMBER,
    DESC_RC_EXT NUMBER,
    SUMA_ASEG_GM_EXT NUMBER,
    PRIMA_GM_EXT NUMBER,
    DESC_GM_EXT NUMBER,
    PRIMA_EXE_DED_X_VEH_IDENT NUMBER,
    PRIMA_AJUSTE_VALOR_COM NUMBER,
    PRIMA_GT_X_PP NUMBER,
    SUMA_ASEG_GT_X_PT NUMBER,
    SUMA_ASEG_GT_X_PP NUMBER,
    DIAS_DED_GT_PT NUMBER,
    DIAS_DED_GT_PP NUMBER,
    SUMA_ASEG_AJ_VAL_COM_X_PT NUMBER,
    PRIMA_EXTENSION_GARANTIA NUMBER,
    PRIMA_EXTENSION_MTTO NUMBER,
    COD_ANIO_EXT_GARANTIA NUMBER,
    PRIMA_ADAPT_SPT NUMBER,
    FECHA_PROCESO DATE,
    PRIMA_EXE_DED_RT NUMBER,
    PROV_ASIST_SAT VARCHAR2(4000),
    SA_GTP_DIARIA NUMBER,
    SA_RC_DANOS_AL_VEH_ARRASTRA NUMBER,
    PMA_RC_DAN_VEH_ARRASTRA NUMBER,
    DIAS_RC_DAN_VEH_ARRASTRA NUMBER,
    DESC_DED_RC_DAN_VEH_ARRASTRA NUMBER,
    AUTORIZA_SOBREPASAR_SA_RC_DVA NUMBER,
    SA_RC_EN_EUA NUMBER,
    PMA_RC_EN_EUA NUMBER,
    DIAS_RC_EUA NUMBER,
    SA_RCP_EUA NUMBER,
    PMA_RCP_EUA NUMBER,
    DIAS_RCP_EUA NUMBER,
    SA_RCB_EUA NUMBER,
    PMA_RCB_EUA NUMBER,
    DIAS_RCB_EUA NUMBER,
    PAQUETE_RCB_EUA NUMBER,
    NUM_ECONOMICO NUMBER,
    SA_AVERIA_MEC_ELEC NUMBER,
    PMA_AVERIA_MEC_ELEC NUMBER,
    SA_AV_AVERIA_MEC_ELEC NUMBER,
    PMA_AV_AVERIA_MEC_ELEC NUMBER,
    SA_AUTO_SUSTITUTO NUMBER,
    PMA_AUTO_SUSTITUTO NUMBER,
    SA_LLANTAS NUMBER,
    PMA_LLANTAS NUMBER,
    SA_RINES NUMBER,
    DED_LLANTAS_Y_RINES NUMBER,
    PMA_RINES NUMBER,
    GAP1 NUMBER,
    SA_CB_FLEX NUMBER,
    DSCTO_COB VARCHAR2(4000),
    AMPARA_P2MIL_PREMIUM VARCHAR2(4000),
    MARCA_VEH_ALTO_RGO VARCHAR2(4000),
    INSP_VEH VARCHAR2(4000),
    DANIOS_PREEXISTENTES VARCHAR2(4000),
    DESC_DANOS_PREEXISTENTES VARCHAR2(4000),
    FEC_INSP_VEH DATE,
    COND_VIG_A_NIVEL_DI VARCHAR2(4000),
    AMPARA_AV_REM_ENGAN VARCHAR2(4000),
    REGISTRO_CNSF VARCHAR2(4000),
    PMA_AV_REMOLQUE_ENGANC VARCHAR2(4000),
    QUIEN_HIZO_INSP_VEH VARCHAR2(4000),
    INSP_VEH_X_OFIC VARCHAR2(4000),
    VEH_BLINDADO VARCHAR2(4000),
    FACTOR_BLINDAJE VARCHAR2(4000),
    GAP2 VARCHAR2(4000),
    SA_RCP_OBLIGATORIA NUMBER,
    PMA_RCP_OBLIG NUMBER,
    DIAS_DED_RCP_OBLIG NUMBER,
    SA_RCB_OBLIG NUMBER,
    PMA_RCB_OBLIG NUMBER,
    DIAS_DED_RCB_OBLIG NUMBER,
    DESC_DED_RC_OBLIG NUMBER,
    DESC_DED_RCP_OBLIG NUMBER,
    PAQUETE_RC_OBLIG VARCHAR2(4000),
    ADICIONALES_AV VARCHAR2(4000),
    AV_MANIOBRAS VARCHAR2(4000),
    RECARGO_AV_MANIOBRAS NUMBER,
    PMA_ANUAL_RC NUMBER,
    PMA_ANUAL_RC_OBLIG NUMBER,
    PMA_TOT_RC_OBLIG NUMBER,
    SA_TOT_ANUAL_RC_OBLIG NUMBER,
    BENEF_PREF_INCISO VARCHAR2(4000),
    SA_SINIESTRALIDAD_CADE NUMBER,
    TIPO_OFIC_INSP_VEH VARCHAR2(4000),
    CUOTA_AUT_SUST_PT NUMBER,
    SUMA_ASEG_AS_PT NUMBER,
    PRIMA_AS_PT NUMBER,
    AUTORIZO_CAMBIO_USO VARCHAR2(4000),
    AUTORIZO_CVES_DIREC_SERIE VARCHAR2(4000),
    MARCA_UBER_CABIFY VARCHAR2(4000),
    SA_VEH_AUTOS_INC_X_PT VARCHAR2(4000),
    CAUSA_INSP_VEH VARCHAR2(4000),
    RGO_DESC_X_USO_SPT NUMBER,
    SUMA_ASEG_AJINFL NUMBER,
    PRIMA_AV NUMBER,
    DESC_DED_RCB_OBLIG NUMBER,
    PMA_RCP NUMBER,
    PORC_DED_RP NUMBER,
    ULTIMO_ENDOSO NUMBER(1),
    FECHA_CARGA DATE
);