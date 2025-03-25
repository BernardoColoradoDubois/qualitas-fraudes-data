-- Asumimos que el esquema INSUMOS ya existe
-- Todas las tablas se crearán en el esquema INSUMOS

-- Drop tables if they exist (en orden inverso de dependencia)
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.pagos_siniestros CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.coberturas CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_ETIQUETA_SINIESTRO CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_SINIESTROS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.agentes CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.gerentes CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_OFICINAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_CAUSAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.vehiculos CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_PAGOS_POLIZAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.polizas_vigentes CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.asegurados CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

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

-- Tabla asegurados
CREATE TABLE INSUMOS.asegurados (
    id VARCHAR2(50) PRIMARY KEY,
    nombre VARCHAR2(255),
    apellido_paterno VARCHAR2(100),
    apellido_materno VARCHAR2(100),
    fecha_alta VARCHAR2(50)
);

-- Tabla polizas_vigentes
CREATE TABLE INSUMOS.polizas_vigentes (
    id VARCHAR2(50) PRIMARY KEY,
    id_asegurado VARCHAR2(50),
    poliza VARCHAR2(50),
    ramo VARCHAR2(50),
    endoso VARCHAR2(50),
    prima_neta NUMBER,
    prima_total NUMBER,
    iva NUMBER,
    subtotal NUMBER,
    fecha_emision DATE,
    vigente_desde DATE,
    vigente_hasta DATE
);


-- Tabla vehiculos
CREATE TABLE INSUMOS.vehiculos (
    id VARCHAR2(50) PRIMARY KEY,
    seccion VARCHAR2(50),
    poliza VARCHAR2(50),
    endoso VARCHAR2(50),
    inciso VARCHAR2(50),
    marca_completa VARCHAR2(255),
    marca_corta VARCHAR2(100),
    subramo VARCHAR2(100),
    modelo VARCHAR2(100),
    origen VARCHAR2(100),
    tipo_vehiculo VARCHAR2(100),
    serie VARCHAR2(100),
    placas VARCHAR2(50),
    tipo_cobertura VARCHAR2(100),
    categoria VARCHAR2(100),
    valor_0km VARCHAR2(100),
    suma_aseg VARCHAR2(100)
);

-- Tabla DM_CAUSAS
CREATE TABLE INSUMOS.DM_CAUSAS (
    ID VARCHAR2(50) PRIMARY KEY,
    CAUSA VARCHAR2(255),
    CAUSA_HOMOLOGADA VARCHAR2(255),
    FECHA_LOTE TIMESTAMP,
    ID_USUARIO_SISTEMA VARCHAR2(50)
);

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

-- Tabla gerentes
CREATE TABLE INSUMOS.gerentes (
    id VARCHAR2(50) PRIMARY KEY,
    nombre VARCHAR2(255)
);

-- Tabla agentes
CREATE TABLE INSUMOS.agentes (
    id VARCHAR2(50) PRIMARY KEY,
    nombre VARCHAR2(255)
);

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



-- Tabla coberturas (con referencias a DM_SINIESTROS)
CREATE TABLE INSUMOS.coberturas (
    id VARCHAR2(50) PRIMARY KEY,
    id_siniestro VARCHAR2(50),
    codigo_cobertura VARCHAR2(50),
    cobertura VARCHAR2(255),
    tipo_movimiento VARCHAR2(100),
    moneda VARCHAR2(50),
    gasto NUMBER,
    importe NUMBER,
    fecha_registro DATE,
    fecha_movimiento DATE
);

-- Tabla pagos_siniestros (con referencias a DM_SINIESTROS)
CREATE TABLE INSUMOS.pagos_siniestros (
    id VARCHAR2(50) PRIMARY KEY,
    id_siniestro VARCHAR2(50),
    importe NUMBER,
    fecha_pago DATE
);

-- Se han removido los índices de claves foráneas
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
    RFC_FACTURACION VARCHAR2(13),
    USUARIO_EMITE VARCHAR2(50),
    STATUS VARCHAR2(10),
    FECHA_PROCESO DATE
);


