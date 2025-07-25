
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_DATOS_VEHICULO CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;


CREATE TABLE INSUMOS.DM_DATOS_VEHICULO
(
    IDDATOSVEHICULO         VARCHAR2(4000) PRIMARY KEY,
    IDEXPEDIENTE            VARCHAR2(4000),
    IDVEHICULO              VARCHAR2(4000),
    IDUSO                   VARCHAR2(4000),
    IDMARCA                 VARCHAR2(4000),
    IDCOLOR                 VARCHAR2(4000),
    CVESUBMARCA             VARCHAR2(4000),
    CVEAMIS                 VARCHAR2(4000),
    TIPO                    VARCHAR2(4000),
    MODELO                  VARCHAR2(4000),
    PLACAS                  VARCHAR2(4000),
    SERIE                   VARCHAR2(4000),
    MOTOR                   VARCHAR2(4000),
    USOAMIS                 VARCHAR2(4000),
    PLACASANTERIOR          VARCHAR2(4000),
    SERIEANTERIOR           VARCHAR2(4000),
    IDUNIDAD                VARCHAR2(4000),
    IDMARCATALLER           VARCHAR2(4000)
);