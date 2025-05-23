BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE RAW_INSUMOS.STG_TIPOS_PROVEEDORES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE RAW_INSUMOS.STG_TIPOS_PROVEEDORES (
    ID VARCHAR2(4000),
    TIPO_PROVEEDOR VARCHAR2(4000),
    GRUPO VARCHAR2(4000),
    NOMBRE_GRUPO VARCHAR2(4000)
);