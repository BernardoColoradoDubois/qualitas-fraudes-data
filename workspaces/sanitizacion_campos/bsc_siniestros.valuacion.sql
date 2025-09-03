SELECT
  VALUACION
  ,[Tipo de Valuacion] AS Tipo_de_Valuacion
  ,Afectado
  ,[Modelo Vehiculo] AS Modelo_Vehiculo
  ,[Tipo Vehiculo] AS Tipo_Vehiculo
  ,[Fecha Ocurrido] AS Fecha_Ocurrido
  ,[Fecha Ingreso] AS Fecha_Ingreso
  ,[Fecha Valuacion] AS Fecha_Valuacion
  ,MONEDA
  ,[Fecha Modificacion] AS Fecha_Modificacion
  ,[Fecha Captura] AS Fecha_Captura
  ,[Fecha Entrega] AS Fecha_Entrega
  ,[Fecha Terminado] AS Fecha_Terminado
  ,[Fecha Promesa] AS Fecha_Promesa
  ,[Numero Valuaciones] AS Numero_Valuaciones
  ,[Importe Demerito] AS Importe_Demerito
  ,[Numero Demeritos] AS Numero_Demeritos
  ,DEDUCIBLE
  ,[Numero Deducibles] AS Numero_Deducibles
  ,[Importe PD] AS Importe_PD
  ,[Numero PD] AS Numero_PD
  ,[Importe PT] AS Importe_PT
  ,[Numero PT] AS Numero_PT
  ,[Numero Reparaciones] AS Numero_Reparaciones
  ,[Importe Valuacion] AS Importe_Valuacion
  ,[Complemento Refacciones] AS Complemento_Refacciones
  ,[Complemento Mano de Obra] AS Complemento_Mano_de_Obra
  ,Complemento
  ,[Inicial Refacciones] AS Inicial_Refacciones
  ,[Inicial Mano de Obra] AS Inicial_Mano_de_Obra
  ,[Importe Reparaciones] AS Importe_Reparaciones
  ,ANTIGUEDAD
  ,[Class Tipo Vehiculo] AS Class_Tipo_Vehiculo
  ,[Fecha Complemento] AS Fecha_Complemento
  ,[Numero de Siniestro] AS Numero_de_Siniestro
  ,[OFICINA ATENCION] AS OFICINA_ATENCION
  ,[Region Atencion] AS Region_Atencion
  ,[tipo proveedor] AS tipo_proveedor
  ,Agente
  ,Gerente
  ,Oficina
  ,Director
  ,[Regional Valuacion] AS Regional_Valuacion
  ,[Tipo Valuacion] AS Tipo_Valuacion
  ,[Tipo Negocio] AS Tipo_Negocio
  ,[Entidad Federativa] AS Entidad_Federativa
  ,Ajustador
  ,POLIZA
  ,INCISO
  ,CAUSA
  ,[Grupo Proveedor] AS Grupo_Proveedor
  ,GRUPOCOMERCIAL
  ,Paquete
  ,Valuador
  ,Proveedor
  ,[Clave Proveedor] AS Clave_Proveedor
  ,ESTATUS_HOMOLO_VAL
  ,[Marca Proveedor] AS Marca_Proveedor
  ,[Marca Vehiculo] AS Marca_Vehiculo
  ,TipoAgencia
  ,TIPOSISE
  ,[Categoria Valuador] AS Categoria_Valuador
  ,VALUACION_TIPO_VEHICULO
  ,REGION_SERVICIO
  ,Status
  ,EstatusProveedor
  ,ClaveValuador
  ,EstatusValuador
  ,TIPO_CAPTURA
  ,COD_CAT_VEH
  ,DESC_CAT_VEH
  ,USO
  ,ClaveAjustador
  ,CAUSA_HOMOLOGADA
  ,SUBRAMO
  ,ESTATUS_VALUACION
  ,REPORTE
  ,[Oficina Beneficiario] AS Oficina_Beneficiario
  ,[Clave Oficina Atencion] AS Clave_Oficina_Atencion
  ,[Clave Oficina Emision] AS Clave_Oficina_Emision
  ,CATVAL
  ,NumPiezas
  ,NumPiezasOriginal
  ,[Region Proveedor] AS Region_Proveedor
  ,[Cuenta Especial] AS Cuenta_Especial
  ,COBERTURA
  ,CVE_AGENTE
  ,[NCuenta Especial] AS NCuenta_Especial
  ,Segmentacion
  ,OFICINA_SUBPLAZA
  ,NumComplementos
  ,[Tiempo de Valuacion] AS Tiempo_de_Valuacion
  ,[Tiempo de Termino] AS Tiempo_de_Termino
  ,[Tiempo de Taller] AS Tiempo_de_Taller
  ,VALVALUACION
  ,VALTERMINO
  ,VALTALLER
  ,SUMAASEG
  ,TIPOPOLIZA
  ,SERIE
  ,CveOficPres
  ,CveEstPres
  ,PoblacionPres
  ,PoblacionComercial
  ,PoblacionComercial2
  ,Estado_Comercial
  ,PISO
  ,TRANSITO
  ,CatValuacion
  ,REF
  ,MO
  ,NOMOFICINA
  ,EJERCICIO
  ,EXPUESTO
  ,CONSUMA
  ,[DESC.COBER] AS DESC_COBER
  ,MARCA_MOTO
  ,[Herramienta Valua] AS Herramienta_Valua
  ,Segmento
  ,[Región Nueva] AS Region_Nueva
  ,[Categoria Nueva] AS Categoria_Nueva
  ,[MARCAS SISE] AS MARCAS_SISE
  ,REPORTECOBE
  ,CONVENIO
  ,SUBDIRECCION
  ,TMPO_REPARA_DIA
  ,QVSQ
  ,DESC_COBERTURA
  ,COD_ESTATUS_VAL
  ,TIPO_VAL_EP
  ,ZONAS
  ,GAMA
  ,NUMVED
  ,VEDPIEZAS
  ,[Fecha Pago] AS Fecha_Pago
  ,CAT_VEH
  ,ZONA
FROM "BSCSiniestros"."dbo"."VALUACION"