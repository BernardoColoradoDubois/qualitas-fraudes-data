CREATE USER INSUMOS 
IDENTIFIED BY INSUMOS  -- Reemplaza "contraseña" con una contraseña segura
DEFAULT TABLESPACE users
TEMPORARY TABLESPACE temp
QUOTA UNLIMITED ON users;

GRANT CONNECT, RESOURCE TO INSUMOS;

-- Paso 3: Otorgar privilegios específicos para trabajar dentro de su propio esquema
GRANT CREATE SESSION TO INSUMOS;
GRANT CREATE TABLE TO INSUMOS;
GRANT CREATE VIEW TO INSUMOS;
GRANT CREATE PROCEDURE TO INSUMOS;
GRANT CREATE TRIGGER TO INSUMOS;
GRANT CREATE SEQUENCE TO INSUMOS;
GRANT CREATE SYNONYM TO INSUMOS;
GRANT CREATE MATERIALIZED VIEW TO INSUMOS;
GRANT CREATE TYPE TO INSUMOS;