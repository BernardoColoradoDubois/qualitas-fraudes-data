SELECT 
    REPLACE(REPLACE(REPLACE(ZID, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS ZID,
    REPLACE(REPLACE(REPLACE(NOMBRE, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS NOMBRE,
    REPLACE(REPLACE(REPLACE(GIRO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS GIRO,
    REPLACE(REPLACE(REPLACE(NACIONALIDAD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS NACIONALIDAD,
    REPLACE(REPLACE(REPLACE(FOLIO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS FOLIO,
    REPLACE(REPLACE(REPLACE(APODERADO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS APODERADO,
    REPLACE(REPLACE(REPLACE(NAC_APODERADO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS NAC_APODERADO,
    REPLACE(REPLACE(REPLACE(RFC, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS RFC,
    REPLACE(REPLACE(REPLACE(ART_140, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS ART_140,
    FEC_NACE,
    FEC_ALTA,
    REPLACE(REPLACE(REPLACE(TIP_ASEG, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TIP_ASEG,
    REPLACE(REPLACE(REPLACE(CORREO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS CORREO,
    REPLACE(REPLACE(REPLACE(CURP, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS CURP,
    REPLACE(REPLACE(REPLACE(OFICINA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS OFICINA,
    REPLACE(REPLACE(REPLACE(NOM_OFICINA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS NOM_OFICINA
FROM 
    BSCSiniestros.dbo.MASEG_BSC