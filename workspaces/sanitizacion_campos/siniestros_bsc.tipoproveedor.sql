SELECT 
    REPLACE(REPLACE(REPLACE(id, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS id,
    REPLACE(REPLACE(REPLACE([tipo proveedor], CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS [tipo proveedor],
    REPLACE(REPLACE(REPLACE(Grupo, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Grupo,
    REPLACE(REPLACE(REPLACE(nombre_grupo, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS nombre_grupo
FROM 
    BSCSiniestros.dbo.tipoProveedor;