BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE INSUMOS.DM_VALUACIONES CASCADE CONSTRAINTS';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;

CREATE TABLE INSUMOS.DM_VALUACIONES (
 VALUACION VARCHAR2(4000) PRIMARY KEY,
    VALUADORES VARCHAR2(4000),
    TIPO_VALUADOR VARCHAR2(4000),
    TIPO_DE_VALUACION VARCHAR2(4000),
    OFICINA_VALUADOR VARCHAR2(4000),
    REGIONAL_VALUACION VARCHAR2(4000),
    PROVEEDORES VARCHAR2(4000),
    ASEG_TERCERO VARCHAR2(4000),
    TIPO_PROVEEDOR VARCHAR2(4000),
    OFICINA_PROVEEDOR VARCHAR2(4000),
    REGION_PROVEEDOR VARCHAR2(4000),
    TIPO_NEGOCIO VARCHAR2(4000),
    DIRECTOR_GERENTE VARCHAR2(4000),
    CLAVE_GERENTE VARCHAR2(4000),
    GERENTE VARCHAR2(4000),
    CLAVE_AGENTE VARCHAR2(4000),
    AGENTE VARCHAR2(4000),
    OFICINA_VENTAS VARCHAR2(4000),
    MODELO_VEHICULO NUMBER,
    MARCAS VARCHAR2(4000),
    TIPO_VEHICULO VARCHAR2(4000),
    FECHA_ASIGNACION DATE,
    FECHA_OCURRIDO DATE,
    FECHA_INGRESO DATE,
    FECHA_VALUACION DATE,
    FECHA_PROMESA DATE,
    FECHA_TERMINADO DATE,
    FECHA_ENTREGA DATE,
    FECHA_CAPTURA DATE,
    FECHA_MODIFICACION DATE,
    MONEDA VARCHAR2(4000),
    ESTATUS_PROCESO VARCHAR2(4000),
    SISTEMA_VALUACION VARCHAR2(4000),
    VALUACIONES NUMBER,
    NUMERO_PT NUMBER,
    IMPORTE_PT NUMBER,
    NUMERO_PD NUMBER,
    IMPORTE_PD NUMBER,
    NUMERO_DEDUCIBLES NUMBER,
    DEDUCIBLE NUMBER,
    NUMERO_DEMERITOS NUMBER,
    IMPORTE_DEMERITO NUMBER,
    DN_MONEDA_NACIONAL_DEDUCIBLE NUMBER,
    NO_PROCEDE VARCHAR2(4000),
    RECHAZADO VARCHAR2(4000),
    POLIZA_PERIODO_GRACIA VARCHAR2(4000),
    REPARACIONES NUMBER,
    INICIAL_MO NUMBER,
    INICIAL_REF NUMBER,
    COMP_MO NUMBER,
    COMP_REF NUMBER,
    TOTAL_VALUACION NUMBER,
    ESTADO VARCHAR2(4000),
    VALUACION_TIPO_VEHICULO VARCHAR2(4000),
    SUMA_ASEGURADA NUMBER,
    IMPORTE_REPARACIONES NUMBER,
    ANTIGUEDAD VARCHAR2(4000),
    AJUSTADOR VARCHAR2(4000),
    OFICINA_AJUSTADOR VARCHAR2(4000),
    REG_AJUSTADOR VARCHAR2(4000),
    CLASE_TIPO_VEHICULO VARCHAR2(4000),
    IDENTIFICADOR VARCHAR2(4000),
    PIEZAS_REPARADAS NUMBER,
    PIEZAS_SUSTITUIDAS NUMBER,
    MONTOS_REPARACION NUMBER,
    MONTOS_SUSTITUCION NUMBER,
    MARCA_PROVEEDOR VARCHAR2(4000),
    FECHA_COMPLE DATE,
    DIAS_VAL_MOD VARCHAR2(4000),
    NUM_COMP_MO NUMBER,
    NUM_COMP_REF NUMBER,
    TIPO_SISE VARCHAR2(4000),
    VALVSCAPTURA VARCHAR2(4000),
    TIPO_CAPTURA VARCHAR2(4000),
    COMPLEMENTOS VARCHAR2(4000),
    SINIESTRO VARCHAR2(4000),
    POLIZA VARCHAR2(4000),
    INCISO VARCHAR2(4000),
    CAUSA VARCHAR2(4000),
    GRUPOCOMERCIAL VARCHAR2(4000),
    COBERTURA VARCHAR2(4000),
    PAQUETE VARCHAR2(4000),
    COD_ESTATUS_VAL VARCHAR2(4000),
    SUBR VARCHAR2(4000),
    CATEGORIA VARCHAR2(4000),
    USO VARCHAR2(4000),
    CODIGO_CATEGORIA_VEHICULO VARCHAR2(4000),
    DESCRIPCION_CATEGORIA_VEHICULO VARCHAR2(4000),
    CODIGO_CAUSA VARCHAR2(4000),
    SERIE VARCHAR2(4000),
    TIEMPO_DE_VALUACION NUMBER,
    TIEMPO_DE_TERMINO NUMBER,
    TIEMPO_DE_TALLER NUMBER,
    REPORTE VARCHAR2(4000),
    REPORTECOBE VARCHAR2(4000),
    NUMERO_PIEZAS NUMBER,
    NUMERO_PIEZAS_ORIGINAL NUMBER,
    NUMERO_COMPLEMENTOS NUMBER,
    SUMAASEG NUMBER,
    TIPO_POLIZA VARCHAR2(4000),
    PISO NUMBER,
    TRANSITO NUMBER,
    CATVAL VARCHAR2(4000),
    DESC_COBER VARCHAR2(4000),
    TP_VAL VARCHAR2(4000),
    CONVENIO NUMBER,
    PT_USUARIO VARCHAR2(4000),
    PT_OFICINA VARCHAR2(4000),
    QVSQ NUMBER,
    FECHA_PAGO DATE
);