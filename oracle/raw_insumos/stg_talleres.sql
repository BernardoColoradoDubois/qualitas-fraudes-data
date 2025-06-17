

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RAW_INSUMOS.STG_TALLERES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE RAW_INSUMOS.STG_TALLERES
(
    IDTALLER                            NUMBER NOT NULL,
    IDOFICINA                           VARCHAR2(4000),
    CLAVETALLER                         VARCHAR2(4000),
    RAZONSOCIAL                         VARCHAR2(4000),
    NOMBRECOMERCIAL                     VARCHAR2(4000),
    CONTRIBUYENTE                       VARCHAR2(4000),
    REPRESENTANTELEGAL                  VARCHAR2(4000),
    CORREO                              VARCHAR2(4000),
    RFC                                 VARCHAR2(4000),
    DOMICILIOFISCAL                     VARCHAR2(4000),
    DIRECCIONCOMERCIAL                  VARCHAR2(4000),
    CELULARNEXTEL                       VARCHAR2(4000),
    TARIFAMOB                           NUMBER,
    ENTIDAD                             VARCHAR2(4000),
    PERFIL                              VARCHAR2(4000),
    COTIZADOR                           NUMBER,
    MITCHELL                            NUMBER,
    TIPO                                VARCHAR2(4000),
    MULTIMARCA                          NUMBER,
    IDMARCA                             VARCHAR2(4000),
    IDENTIDAD                           VARCHAR2(4000),
    ENVIOPRESUPUESTO                    NUMBER,
    NUMTELEFONICO                       VARCHAR2(4000),
    MENSAJE                             NUMBER,
    FRONTERIZO                          NUMBER,
    FECHAACEPTACIONPOLITICA             DATE,
    ESTATUSTALLER                       NUMBER,
    FECACEPTACIONPOLITICAEQPESADO       DATE,
    PAGOANTICIPADO                      NUMBER,
    USUARIOHERRAMIENTA                  VARCHAR2(4000),
    ACTIVARAUDACLAIMS                   NUMBER NOT NULL,
    COMPANIAASIGNADAACG                 VARCHAR2(4000),
    POOLASIGNADOACG                     VARCHAR2(4000),
    USUARIOALTA                         VARCHAR2(4000),
    USUARIOACTUALIZACION                VARCHAR2(4000),
    ALTA                                DATE,
    ACTUALIZACION                       DATE,
    IDESTADO                            VARCHAR2(4000),
    IMAGENQCONTENT                      VARCHAR2(4000),
    ANUNCIOS_PORTAL                     NUMBER,
    FECCAMPANIALIM                      DATE,
    RECCHATARRA                         VARCHAR2(4000),
    FECACEPTAETICACONDUCTATALLER        DATE,
    FECACEPTACONFLICTOTALLER            DATE,
    ACTIVO                              NUMBER,
    MAXIMO                              NUMBER,
    MINIMO                              NUMBER,
    RPM                                 NUMBER,
    LIBERADO                            VARCHAR2(4000),
    FECACEPTAAPPMOVIL                   DATE,
    ACTIVAPROORDER                      VARCHAR2(4000),
    FECACEPTAAVISOPRIV                  DATE,
    FECACEPTAAVISOPO                    DATE,
    IDTIPOTALLER                        VARCHAR2(4000),
    ACTIVAXNUUP                         NUMBER
);
