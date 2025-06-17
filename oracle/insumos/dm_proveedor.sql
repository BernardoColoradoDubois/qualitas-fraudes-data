BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_PROVEEDOR CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_PROVEEDOR
(
    CODPROVEEDOR                        VARCHAR2(4000) NOT NULL PRIMARY KEY,
    NOMBRE                              VARCHAR2(4000) NOT NULL,
    ALTA                                DATE NOT NULL,
    ACTUALIZACION                       DATE,
    USUARIOACTUALIZA                    VARCHAR2(4000),
    IDREGION                            VARCHAR2(4000),
    MAIL                                VARCHAR2(4000),
    DATO                                VARCHAR2(4000),
    TIPO                                VARCHAR2(4000),
    MULTIMARCA                          NUMBER,
    IDTIPOTOT                           VARCHAR2(4000),
    IDIVA                               VARCHAR2(4000),
    CELULARNEXTEL                       VARCHAR2(4000),
    NUMTELEFONICO                       VARCHAR2(4000),
    ASIGNACIONVALUADOR                  NUMBER,
    PORCENTAJEDESCUENTOADICIONAL        NUMBER,
    COSTOFIJO                           NUMBER,
    FECHAACEPTACIONPOLITICA             DATE,
    FIRMA                               NUMBER,
    ESTATUSPROVEEDOR                    NUMBER,
    RFC                                 VARCHAR2(4000),
    USUARIOALTA                         VARCHAR2(4000),
    RECCHATARRA                         VARCHAR2(4000),
    FECACEPTAETICACONDUCTAPROV          DATE,
    FECACEPTACONFLICTOPROV              DATE,
    VIP                                 NUMBER,
    VERINDICADORES                      VARCHAR2(4000),
    SUSPAUTO                            VARCHAR2(4000),
    NOMOSTRARMONTOENVALES               NUMBER NOT NULL,
    LIBERADO                            VARCHAR2(4000),
    MAIL2                               VARCHAR2(4000),
    FECACEPTAAVISOPRIV                  DATE,
    CONFIANZA                           VARCHAR2(4000),
    ACTUALIZACIONCONFIANZA              DATE,
    ASIGNACIONINACTIVA                  VARCHAR2(4000)
);