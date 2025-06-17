

BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RAW_INSUMOS.STG_FECHAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE RAW_INSUMOS.STG_FECHAS
(
    IDFECHAS                        VARCHAR2(4000),
    IDEXPEDIENTE                    VARCHAR2(4000),
    FECASIGNACION                   DATE,
    FECADJUDICACION                 DATE,
    FECINGRESO                      DATE,
    FECVALUACION                    DATE,
    FECTERMINADO                    DATE,
    FECENTREGADO                    DATE,
    FECREGISTRO                     DATE,
    FECENVIO                        DATE,
    DIASREPARACION                  NUMBER,
    FECASIGNACIONVAL                DATE,
    FECAUTORIZACION                 DATE,
    FECSINIESTRO                    DATE,
    FECPROMESA                      DATE,
    FECREFACCIONESENTREGADAS        DATE,
    FECREGISTROLLAVECRM             DATE,
    FECACTUALIZACIONLLAVECRM        DATE,
    FECREGISTROPAGOANTICIPADO       DATE,
    FECENVIOCARRUSELA8              DATE,
    FECENVIOCARRUSELBLINDAJE        DATE,
    FECPRIMERENVIOHERRAMIENTA       DATE,
    FECHAENVIOPT                    DATE,
    FECHAPROGPT                     DATE,
    FECMARCADOUSUARIO               DATE,
    FECADJUDICACCQ                  DATE,
    PROMESA_ENVIO_CRM               DATE,
    FECHAFINIQUITO                  DATE,
    INGRESO_CLIENTE                 DATE,
    FECLIBCHATARRA                  DATE,
    FECHAOCURRIDOSISE               DATE,
    AUT_UNA_PZA_CM                  DATE,
    FECHAESTENT                     DATE,
    SEMANAESTENT                    VARCHAR2(4000),
    ESTIMADAENVIOCRM                DATE
);
