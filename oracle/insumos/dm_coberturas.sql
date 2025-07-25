BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_COBERTURAS CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

-- Tabla DM_CAUSAS
CREATE TABLE INSUMOS.DM_COBERTURAS (
    ID VARCHAR2(50) PRIMARY KEY,
    COBERTURA VARCHAR2(255),
    COBERTURA_HOMOLOGADA VARCHAR2(255)
);