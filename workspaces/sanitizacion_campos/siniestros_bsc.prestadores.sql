SELECT
    Id,
    REPLACE(REPLACE(REPLACE(Nombre, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Nombre,
    REPLACE(REPLACE(REPLACE(Marca, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Marca,
    REPLACE(REPLACE(REPLACE(Tipo, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Tipo,
    REPLACE(REPLACE(REPLACE(Oficina, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Oficina,
    REPLACE(REPLACE(REPLACE(SUSPENCION, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS SUSPENCION,
    REPLACE(REPLACE(REPLACE(Status, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Status,
    Fbaja,
    REPLACE(REPLACE(REPLACE(CodEstado, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS CodEstado,
    REPLACE(REPLACE(REPLACE(Poblacion, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Poblacion,
    REPLACE(REPLACE(REPLACE(Direccion, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Direccion,
    REPLACE(REPLACE(REPLACE(Colonia, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Colonia,
    REPLACE(REPLACE(REPLACE(Telefono, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Telefono,
    REPLACE(REPLACE(REPLACE(Dir_Comer, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Dir_Comer,
    REPLACE(REPLACE(REPLACE(Col_Comer, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Col_Comer,
    REPLACE(REPLACE(REPLACE(Pob_Comer, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Pob_Comer,
    REPLACE(REPLACE(REPLACE(Edo_Comer, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Edo_Comer,
    REPLACE(REPLACE(REPLACE(Num_Docto, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Num_Docto,
    REPLACE(REPLACE(REPLACE(Nom_Comer, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Nom_Comer,
    REPLACE(REPLACE(REPLACE(Grupo, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Grupo,
    FEC_ALTA,
    REPLACE(REPLACE(REPLACE(CTA_ESP, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS CTA_ESP,
    REPLACE(REPLACE(REPLACE(TIPO_AJUS, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TIPO_AJUS,
    REPLACE(REPLACE(REPLACE(MARCA_TIPO, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS MARCA_TIPO,
    REPLACE(REPLACE(REPLACE(RFC, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS RFC,
    REPLACE(REPLACE(REPLACE(Correo_Comercial, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Correo_Comercial,
    REPLACE(REPLACE(REPLACE(Correo_Contacto, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Correo_Contacto,
    REPLACE(REPLACE(REPLACE(MARCA_MOTOS, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS MARCA_MOTOS,
    REPLACE(REPLACE(REPLACE(TEL_SIICA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TEL_SIICA,
    AE,
    REPLACE(REPLACE(REPLACE(LATITUD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS LATITUD,
    REPLACE(REPLACE(REPLACE(LONGITUD, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS LONGITUD,
    REPLACE(REPLACE(REPLACE(EDOCOM, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS EDOCOM,
    REPLACE(REPLACE(REPLACE(POBCOMER, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS POBCOMER,
    REPLACE(REPLACE(REPLACE(RECIBE24, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS RECIBE24,
    REPLACE(REPLACE(REPLACE(ZONAPROV, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS ZONAPROV,
    REPLACE(REPLACE(REPLACE(VED, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS VED,
    REPLACE(REPLACE(REPLACE(TIPO_PERSONA, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TIPO_PERSONA,
    REPLACE(REPLACE(REPLACE(Correo_Comercial2, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS Correo_Comercial2,
    CEXP,
    REPLACE(REPLACE(REPLACE(TBODY, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS TBODY,
    REPLACE(REPLACE(REPLACE(NUM_EMPL, CHAR(10), ''), CHAR(13), ''), CHAR(9), '') AS NUM_EMPL
FROM
    BSCSiniestros.dbo.Prestadores;