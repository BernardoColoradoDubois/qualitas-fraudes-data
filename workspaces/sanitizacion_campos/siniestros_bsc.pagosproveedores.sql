SELECT
    [Clave],
    REPLACE(REPLACE(REPLACE([Status Proveedor], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Status_Proveedor],
    REPLACE(REPLACE(REPLACE([Grupo Proveedor], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Grupo_Proveedor],
    REPLACE(REPLACE(REPLACE([tipo proveedor], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [tipo_proveedor],
    REPLACE(REPLACE(REPLACE([TRAMITE], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [TRAMITE],
    [Importe Mano de Obra] AS [Importe_Mano_de_Obra],
    [Importe Refacciones] AS [Importe_Refacciones],
    REPLACE(REPLACE(REPLACE([Region Servicio Provedor], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Region_Servicio_Provedor],
    REPLACE(REPLACE(REPLACE([OFICINA_BENEF], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [OFICINA_BENEF],
    REPLACE(REPLACE(REPLACE([Clave Proveedor], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Clave_Proveedor],
    REPLACE(REPLACE(REPLACE([Beneficiario], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Beneficiario],
    REPLACE(REPLACE(REPLACE([Documento Pago], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Documento_Pago],
    [Importe Pago] AS [Importe_Pago],
    REPLACE(REPLACE(REPLACE([Orden de Pago], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Orden_de_Pago],
    REPLACE(REPLACE(REPLACE([Codigo de Pago], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Codigo_de_Pago],
    REPLACE(REPLACE(REPLACE([Tipo Gasto], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Tipo_Gasto],
    [Fecha Pago] AS [Fecha_Pago],
    REPLACE(REPLACE(REPLACE([Moneda], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Moneda],
    REPLACE(REPLACE(REPLACE([Cobertura], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Cobertura],
    REPLACE(REPLACE(REPLACE([Poliza], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Poliza],
    REPLACE(REPLACE(REPLACE([Endoso], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Endoso],
    REPLACE(REPLACE(REPLACE([Inciso], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Inciso],
    REPLACE(REPLACE(REPLACE([SUBRAMO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [SUBRAMO],
    REPLACE(REPLACE(REPLACE([Tipo Vehiculo], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Tipo_Vehiculo],
    REPLACE(REPLACE(REPLACE([Uso Vehiculo], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Uso_Vehiculo],
    REPLACE(REPLACE(REPLACE([Servicio Vehiculo], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Servicio_Vehiculo],
    REPLACE(REPLACE(REPLACE([UNIDAD], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [UNIDAD],
    REPLACE(REPLACE(REPLACE([MODELO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [MODELO],
    [Prima Poliza] AS [Prima_Poliza],
    REPLACE(REPLACE(REPLACE([Asegurado], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Asegurado],
    REPLACE(REPLACE(REPLACE([Agente], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Agente],
    REPLACE(REPLACE(REPLACE([Gerente], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Gerente],
    REPLACE(REPLACE(REPLACE([Oficina], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Oficina],
    REPLACE(REPLACE(REPLACE([Director], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Director],
    REPLACE(REPLACE(REPLACE([Nro. Siniestro], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Nro_Siniestro],
    [Fecha Ocurrido] AS [Fecha_Ocurrido],
    REPLACE(REPLACE(REPLACE([Ajustador], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Ajustador],
    REPLACE(REPLACE(REPLACE([OFICINA ATENCION], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [OFICINA_ATENCION],
    REPLACE(REPLACE(REPLACE([Region de Atencion], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Region_de_Atencion],
    REPLACE(REPLACE(REPLACE([Causa], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Causa],
    [FEC_REG],
    REPLACE(REPLACE(REPLACE([Marca Proveedor], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Marca_Proveedor],
    REPLACE(REPLACE(REPLACE([CVE BENEF], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [CVE_BENEF],
    REPLACE(REPLACE(REPLACE([ORIGEN], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [ORIGEN],
    [IMP_DED],
    [IMP_TOT],
    REPLACE(REPLACE(REPLACE([CVE_DOCTO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [CVE_DOCTO],
    REPLACE(REPLACE(REPLACE([AAAA], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [AAAA],
    REPLACE(REPLACE(REPLACE([MM], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [MM],
    REPLACE(REPLACE(REPLACE([AAAAMM], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [AAAAMM],
    REPLACE(REPLACE(REPLACE([Marca Vehiculo], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Marca_Vehiculo],
    REPLACE(REPLACE(REPLACE([COD_GASTO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [COD_GASTO],
    REPLACE(REPLACE(REPLACE([COD_TXT], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [COD_TXT],
    REPLACE(REPLACE(REPLACE([Cve_tipo_prove], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Cve_tipo_prove],
    [CAPUFE],
    [GRUAS],
    [CONSTRUCTORAS],
    [CRISTALES],
    REPLACE(REPLACE(REPLACE([ADMINISTRADORA_GM], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [ADMINISTRADORA_GM],
    REPLACE(REPLACE(REPLACE([CVE_AJU], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [CVE_AJU],
    REPLACE(REPLACE(REPLACE([REGION_SERVICIO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [REGION_SERVICIO],
    REPLACE(REPLACE(REPLACE([Nom_Comer], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Nom_Comer],
    REPLACE(REPLACE(REPLACE([Grupo], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Grupo],
    REPLACE(REPLACE(REPLACE([GERENCIA_SERVICIO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [GERENCIA_SERVICIO],
    REPLACE(REPLACE(REPLACE([OFICINA_SUBPLAZA], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [OFICINA_SUBPLAZA],
    [Medio Ambiente] AS [Medio_Ambiente],
    REPLACE(REPLACE(REPLACE([PK_FECPAGO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [PK_FECPAGO],
    REPLACE(REPLACE(REPLACE([PK_FECREG], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [PK_FECREG],
    REPLACE(REPLACE(REPLACE([PK_FECOCU], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [PK_FECOCU],
    [ContStro],
    REPLACE(REPLACE(REPLACE([Forma de Pago], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Forma_de_Pago],
    REPLACE(REPLACE(REPLACE([BANCO], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [BANCO],
    REPLACE(REPLACE(REPLACE([CHEQUE], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [CHEQUE],
    REPLACE(REPLACE(REPLACE([CARATULA], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [CARATULA],
    REPLACE(REPLACE(REPLACE([Clave_agente], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Clave_agente],
    REPLACE(REPLACE(REPLACE([Cve_Ofic_Emision], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Cve_Ofic_Emision],
    REPLACE(REPLACE(REPLACE([Cve_Ofic_Atencion], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Cve_Ofic_Atencion],
    REPLACE(REPLACE(REPLACE([Estado de Atencion], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Estado_de_Atencion],
    REPLACE(REPLACE(REPLACE([Tipo Contrato], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Tipo_Contrato],
    [AUTOS],
    [EP],
    [MUERTE],
    [LESIONES],
    [MEDICOS],
    REPLACE(REPLACE(REPLACE([GPO.SUP], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [GPO_SUP],
    REPLACE(REPLACE(REPLACE([Pob_Comer], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [Pob_Comer],
    REPLACE(REPLACE(REPLACE([EDOPOB], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [EDOPOB],
    REPLACE(REPLACE(REPLACE([ENTIDAD], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [ENTIDAD],
    [NUM_NAGS],
    REPLACE(REPLACE(REPLACE([SUBDIRECCION], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [SUBDIRECCION],
    REPLACE(REPLACE(REPLACE([GERENCIA], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [GERENCIA],
    REPLACE(REPLACE(REPLACE([VALUACION], CHAR(13), ''), CHAR(10), ''), CHAR(9), '') AS [VALUACION],
    [TMPO_PAGO_PT]
FROM 
    "BSCSiniestros"."dbo"."PAGOSPROVEEDORES"
WHERE 1=1
AND [Fecha Pago] BETWEEN '2025-03-01' AND '2025-03-31'
AND $CONDITIONS