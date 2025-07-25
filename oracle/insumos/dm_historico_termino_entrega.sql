BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_HISTORICO_TERMINO_ENTREGA CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_HISTORICO_TERMINO_ENTREGA
(
    IDHISTORICOTERMINOENTREGA   VARCHAR2(4000) PRIMARY KEY,
    IDEXPEDIENTE                VARCHAR2(4000),
    TIPOFECHA                   VARCHAR2(4000),
    FECHA                       DATE
);