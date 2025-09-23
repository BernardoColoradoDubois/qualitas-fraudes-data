-- =====================================================
-- TRADUCCIÓN DE SAS SQL A BIGQUERY
-- Tablas intermedias: STG_PREVENCION_FRAUDES
-- Tabla final: DM_PREVENCION_FRAUDES.USUARIOHOMOLOGADO
-- =====================================================

--qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev

-- 1. SUPERVISOR_SEGUIMENTO
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.SUPERVISOR_SEGUIMENTO` AS
SELECT 
    t1.CLAVESUPERVISOR AS CLAVE,
    t1.NOMBRESUPERVISOR AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.SUPERVISORINTEGRAL` t1
WHERE UPPER(t1.CLAVESUPERVISOR) LIKE '%SUPSEGQ%'
ORDER BY t1.NOMBRESUPERVISOR;

-- 2. SUPERVISOR_SERVICIOS
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.SUPERVISOR_SERVICIOS` AS
SELECT 
    t1.CLAVEANALISTA AS CLAVE,
    t1.NOMBRENNALISTA AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ANALISTACDR` t1
WHERE UPPER(t1.CLAVEANALISTA) LIKE '%SUPQ%'
ORDER BY t1.CLAVEANALISTA;

-- 3. SS1
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.SS1` AS
SELECT 
    t1.CLAVEANALISTA AS CLAVE,
    t1.NOMBRENNALISTA AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ANALISTACDR` t1
WHERE UPPER(t1.CLAVEANALISTA) LIKE '%ADMINCDR%';

-- 4. ASR
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ASR` AS
SELECT 
    t1.IDADMINISTRADORREFACCIONES AS CLAVE,
    t1.NOMBRE AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ADMINISTRADORREFACCIONES` t1
WHERE UPPER(t1.IDADMINISTRADORREFACCIONES) LIKE '%ASR%'
ORDER BY t1.NOMBRE;

-- 5. COMP
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.COMP` AS
SELECT 
    t1.IDADMINISTRADORREFACCIONES AS CLAVE,
    t1.NOMBRE AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ADMINISTRADORREFACCIONES` t1
WHERE UPPER(t1.IDADMINISTRADORREFACCIONES) LIKE '%ADMINREFC%';

-- 6. ADMIN
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ADMIN` AS
SELECT 
    t1.IDADMINISTRADORREFACCIONES AS CLAVE,
    t1.NOMBRE AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ADMINISTRADORREFACCIONES` t1
WHERE UPPER(t1.IDADMINISTRADORREFACCIONES) LIKE '%ADMIN%';

-- 7. PERSONALIZADO
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.PERSONALIZADO` AS
SELECT 
    t1.IDADMINISTRADORREFACCIONES AS CLAVE,
    t1.NOMBRE AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ADMINISTRADORREFACCIONES` t1
WHERE t1.IDADMINISTRADORREFACCIONES IN (
    'msalazar',
    'otoledo',
    'KGONZALEZ'
);

-- 8. CORCOMSEG
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.CORCOMSEG` AS
SELECT 
    t1.IDADMINISTRADORREFACCIONES AS CLAVE,
    t1.NOMBRE AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.ADMINISTRADORREFACCIONES` t1
WHERE UPPER(t1.IDADMINISTRADORREFACCIONES) LIKE '%CORCOMSEG%';

-- 9. VALUADORES
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.VALUADORES` AS
SELECT 
    t1.CODVALUADOR AS CLAVE,
    t3.Nombre AS USUARIO
FROM `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_prevencion_fraudes_dev.VALUADOR` t1
LEFT JOIN `qlts-dev-mx-au-bro-verificacio.qlts_bro_op_verificaciones_dev.PRESTADORES` t3 
    ON t1.CODVALUADOR = t3.Id
WHERE t3.Tipo = '45';

-- 10. TABLA CONSOLIDADA (APPEND_TABLE_0005)
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.APPEND_TABLE_0005` AS
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.SUPERVISOR_SEGUIMENTO`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.SUPERVISOR_SERVICIOS`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.VALUADORES`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.SS1`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.COMP`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ADMIN`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.ASR`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.PERSONALIZADO`
UNION ALL
SELECT * FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.CORCOMSEG`;

-- 11. TABLA FINAL USUARIOHOMOLOGADO
CREATE OR REPLACE TABLE `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.USUARIOHOMOLOGADO` AS
SELECT DISTINCT 
    t1.CLAVE AS UsuarioBase,
    t1.USUARIO AS UsuarioHomologado
FROM `qlts-dev-mx-au-pla-verificacio.qlts_pla_op_prevencion_fraudes_dev.APPEND_TABLE_0005` t1;