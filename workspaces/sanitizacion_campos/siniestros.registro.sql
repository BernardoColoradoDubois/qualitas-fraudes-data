SELECT
    REPLACE (
        REPLACE (REPLACE (SINIESTRO, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS SINIESTRO,
    REPLACE (
        REPLACE (REPLACE (ID_OF, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS ID_OF,
    REPLACE (
        REPLACE (REPLACE (DESC_OF, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS DESC_OF,
    REPLACE (
        REPLACE (REPLACE (USUARIO, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS USUARIO,
    REPLACE (
        REPLACE (REPLACE (NOMBRE, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS NOMBRE,
    REPLACE (
        REPLACE (REPLACE (CORREO, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS CORREO,
    REPLACE (
        REPLACE (REPLACE (MOTIVO, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS MOTIVO,
    REPLACE (
        REPLACE (
            REPLACE (OBSERVACIONES, CHR (10), ''),
            CHR (13),
            ''
        ),
        CHR (9),
        ''
    ) AS OBSERVACIONES,
    REPLACE (
        REPLACE (
            REPLACE (DOC_ADICIONAL, CHR (10), ''),
            CHR (13),
            ''
        ),
        CHR (9),
        ''
    ) AS DOC_ADICIONAL,
    REPLACE (
        REPLACE (REPLACE (SIN_SNA, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS SIN_SNA,
    REPLACE (
        REPLACE (REPLACE (ID_ANALISTA, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS ID_ANALISTA,
    FECHA_ASIG,
    REPLACE (
        REPLACE (REPLACE (AREA, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS AREA,
    REPLACE (
        REPLACE (REPLACE (PUESTO, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS PUESTO,
    REPLACE (
        REPLACE (REPLACE (ID_MOTIVO, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS ID_MOTIVO,
    REPLACE (
        REPLACE (REPLACE (MARCA, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS MARCA,
    MONTO_ESTIMADO,
    BATCHDATE,
    REPLACE (
        REPLACE (REPLACE (SYSUSERID, CHR (10), ''), CHR (13), ''),
        CHR (9),
        ''
    ) AS SYSUSERID
FROM
    SASMXAP.REGISTRO;