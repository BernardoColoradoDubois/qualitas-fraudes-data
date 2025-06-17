

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RAW_INSUMOS.STG_MARCA CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE RAW_INSUMOS.STG_MARCA
(
    IDMARCA                     VARCHAR2(4000) NOT NULL,
    DESCRIPCION                 VARCHAR2(4000),
    ALTA                        DATE,
    ACTUALIZACION               DATE,
    USUARIOACTUALIZA            VARCHAR2(4000),
    RESCATEPT                   NUMBER,
    APLICACONVENIO              NUMBER,
    TIEMPOMARCA                 NUMBER,
    AUTOS                       NUMBER NOT NULL,
    EP                          NUMBER NOT NULL,
    CHECKCALFECHAEE             VARCHAR2(4000),
    V0                          NUMBER,
    V1                          NUMBER,
    V2                          NUMBER,
    V3                          NUMBER,
    V4                          NUMBER,
    PORCENTAJE_V0               NUMBER,
    PORCENTAJE_V1               NUMBER,
    PORCENTAJE_V2               NUMBER,
    PORCENTAJE_V3               NUMBER,
    PORCENTAJE_V4               NUMBER,
    ACTIVAMARCAPT               VARCHAR2(4000),
    VESPECIALIZADO              VARCHAR2(4000),
    ALTAESPECIALIZADOMM         DATE,
    ALTAESPECIALIZADO           DATE
);
