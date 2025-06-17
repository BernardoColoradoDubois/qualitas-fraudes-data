BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_ESTATUS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;


CREATE TABLE INSUMOS.DM_ESTATUS
(
    IDESTATUS                       VARCHAR2(4000) PRIMARY KEY,
    IDEXPEDIENTE                    VARCHAR2(4000),
    IDESTATUSEXPEDIENTE             VARCHAR2(4000),
    IDRECHAZO                       VARCHAR2(4000),
    IDPENDIENTE                     VARCHAR2(4000),
    TRANSITO                        NUMBER,
    PISO                            NUMBER,
    GRUA                            NUMBER,
    FECESTATUSEXPEDIENTE            DATE,
    MANUAL                          NUMBER,
    SISE                            NUMBER,
    ENVIOSSISE                      NUMBER,
    SISEMENSAJE                     VARCHAR2(4000),
    COTIZADOR                       NUMBER,
    COTIZADORESTATUS                VARCHAR2(4000),
    AUTORIZACIONCOORDINADOR         NUMBER,
    AUTORIZACIONUSUARIO             VARCHAR2(4000),
    PRESUPUESTOENHERRAMIENTA        NUMBER,
    ADMINREF                        VARCHAR2(4000),
    REASIGNADOVALUADOR              NUMBER,
    IDMOVOPERADOR                   VARCHAR2(4000),
    IDRECHAZOCOMPLEMENTO            VARCHAR2(4000),
    CAMBIOREGIONEXP                 VARCHAR2(4000),
    PROCESOHERRAMIENTA              NUMBER,
    VECESRECHAZADO                  NUMBER,
    PAGOANTICIPADO                  NUMBER,
    REVISIONMASTERPT                VARCHAR2(4000),
    IDRESCATEMASTERPT               VARCHAR2(4000),
    REVISIONBLINDAJES               VARCHAR2(4000),
    ENVIOSHERRAMIENTA               NUMBER,
    HISTOSINSINIESTRO               VARCHAR2(4000),
    HISTOINVESTIGACION              VARCHAR2(4000),
    FECSINSINIESTRO                 DATE,
    FECINVESTIGACION                DATE,
    HISTOACTUALIZACIONSINIESTRO     DATE,
    REGRESOVALEP                    VARCHAR2(4000),
    FECHACANCELACION                DATE,
    ISPIEZASCANCELADAS              VARCHAR2(4000),
    XNUUP                           NUMBER
);
