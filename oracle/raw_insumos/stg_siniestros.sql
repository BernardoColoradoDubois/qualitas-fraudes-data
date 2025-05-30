BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RAW_INSUMOS.STG_SINIESTROS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

-- Tabla DM_SINIESTROS (con referencias a tablas ya definidas)
CREATE TABLE RAW_INSUMOS.STG_SINIESTROS (
    ID VARCHAR2(100),
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