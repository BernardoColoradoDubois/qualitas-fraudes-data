# Definiciones de Tablas - Sistema de Gestión de Seguros/Fraudes

## 1. asegurados
Almacena la información de los clientes asegurados.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del asegurado  |
| nombre            | VARCHAR      |             | Nombre del asegurado               |
| apellido_paterno  | VARCHAR      |             | Apellido paterno del asegurado     |
| apellido_materno  | VARCHAR      |             | Apellido materno del asegurado     |
| fecha_alta        | DATE         |             | Fecha en que se registró el asegurado |

## 2. polizas_vigentes
Almacena la información de las pólizas activas en el sistema.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único de la póliza   |
| id_asegurado      | VARCHAR      | FK          | Referencia al asegurado titular    |
| poliza            | VARCHAR      |             | Número de póliza                   |
| ramo              | VARCHAR      |             | Categoría/ramo del seguro          |
| endoso            | VARCHAR      |             | Número de endoso                   |
| prima_neta        | DECIMAL      |             | Monto de la prima neta             |
| prima_total       | DECIMAL      |             | Monto total de la prima            |
| iva               | DECIMAL      |             | Impuesto al valor agregado         |
| subtotal          | DECIMAL      |             | Subtotal del costo de la póliza    |
| fecha_emision     | DATE         |             | Fecha en que se emitió la póliza   |
| vigente_desde     | DATE         |             | Fecha de inicio de vigencia        |
| vigente_hasta     | DATE         |             | Fecha de fin de vigencia           |

## 3. pagos_polizas
Registra los pagos realizados para cada póliza.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del pago       |
| id_poliza         | VARCHAR      | FK          | Referencia a la póliza             |
| seccion           | VARCHAR      |             | Sección de la póliza               |
| endoso            | VARCHAR      |             | Número de endoso                   |
| poliza            | VARCHAR      |             | Número de póliza                   |
| numero_cobranza   | VARCHAR      |             | Número de referencia de cobranza   |
| prima_neta        | DECIMAL      |             | Monto de la prima neta             |
| iva               | DECIMAL      |             | Impuesto al valor agregado         |
| importe           | DECIMAL      |             | Importe total del pago             |
| remesa            | VARCHAR      |             | Identificador de la remesa         |
| fecha_vencimiento | DATE         |             | Fecha límite para el pago          |
| fecha_aplicacion  | DATE         |             | Fecha en que se aplicó el pago     |
| fecha_carga       | DATE         |             | Fecha en que se registró en el sistema |

## 4. vehiculos
Almacena la información de los vehículos asegurados.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del vehículo   |
| seccion           | VARCHAR      |             | Sección de la póliza               |
| poliza            | VARCHAR      |             | Número de póliza                   |
| endoso            | VARCHAR      |             | Número de endoso                   |
| inciso            | VARCHAR      |             | Inciso del vehículo en la póliza   |
| marca_completa    | VARCHAR      |             | Nombre completo de la marca        |
| marca_corta       | VARCHAR      |             | Nombre abreviado de la marca       |
| subramo           | VARCHAR      |             | Subcategoría del seguro            |
| modelo            | VARCHAR      |             | Año/modelo del vehículo            |
| origen            | VARCHAR      |             | País de origen del vehículo        |
| tipo_vehiculo     | VARCHAR      |             | Categoría del vehículo             |
| serie             | VARCHAR      |             | Número de serie (VIN)              |
| placas            | VARCHAR      |             | Matrícula del vehículo             |
| tipo_cobertura    | VARCHAR      |             | Tipo de cobertura contratada       |
| categoria         | VARCHAR      |             | Categoría del vehículo             |
| valor_0km         | VARCHAR      |             | Valor del vehículo cuando nuevo    |
| suma_aseg         | VARCHAR      |             | Suma asegurada                     |

## 5. proveedores
Almacena información sobre proveedores de servicios (talleres, ajustadores, etc.).

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del proveedor  |
| nombre            | VARCHAR      |             | Nombre del proveedor               |
| tipo              | VARCHAR      |             | Tipo de proveedor                  |
| fecha_alta        | DATE         |             | Fecha de registro del proveedor    |

## 6. agentes
Almacena información sobre los agentes de seguros.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del agente     |
| nombre            | VARCHAR      |             | Nombre completo del agente         |

## 7. gerentes
Almacena información sobre los gerentes que supervisan los siniestros.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del gerente    |
| nombre            | VARCHAR      |             | Nombre completo del gerente        |

## 8. oficinas
Almacena información sobre las oficinas que gestionan los siniestros.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único de la oficina  |
| nombre            | VARCHAR      |             | Nombre de la oficina               |

## 9. causas
Almacena los tipos de causas que pueden originar un siniestro.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único de la causa    |
| nombre            | VARCHAR      |             | Descripción de la causa            |

## 10. siniestros
Almacena la información principal de los siniestros reportados.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del siniestro  |
| id_gerente        | VARCHAR      | FK          | Referencia al gerente asignado     |
| id_oficina        | VARCHAR      | FK          | Referencia a la oficina que gestiona |
| id_agente         | VARCHAR      | FK          | Referencia al agente asignado      |
| id_causa          | VARCHAR      | FK          | Referencia a la causa del siniestro |
| id_proveedor      | VARCHAR      | FK          | Referencia al proveedor de servicio |
| id_vehiculo       | VARCHAR      | FK          | Referencia al vehículo involucrado |
| fecha_reporte     | DATE         |             | Fecha en que se reportó el siniestro |
| fecha_ocurrido    | DATE         |             | Fecha en que ocurrió el siniestro  |
| fecha_registro    | DATE         |             | Fecha de registro en el sistema    |
| latitud           | DECIMAL      |             | Coordenada geográfica - latitud    |
| longitud          | DECIMAL      |             | Coordenada geográfica - longitud   |

## 11. etiquetas_sise
Almacena los estados o etiquetas asignadas a los siniestros.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único de la etiqueta |
| id_siniestro      | VARCHAR      | FK          | Referencia al siniestro            |
| codigo_estatus    | VARCHAR      |             | Código del estado/etiqueta         |
| fecha_carga       | DATE         |             | Fecha de asignación de la etiqueta |

## 12. coberturas
Almacena información sobre las coberturas aplicadas en cada siniestro.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único de la cobertura |
| id_siniestro      | VARCHAR      | FK          | Referencia al siniestro            |
| codigo_cobertura  | VARCHAR      |             | Código de la cobertura aplicada    |
| cobertura         | VARCHAR      |             | Descripción de la cobertura        |
| tipo_movimiento   | VARCHAR      |             | Tipo de movimiento contable        |
| moneda            | VARCHAR      |             | Moneda en que se expresa el monto  |
| gasto             | DECIMAL      |             | Gasto asociado                     |
| importe           | DECIMAL      |             | Importe total                      |
| fecha_registro    | DATE         |             | Fecha de registro en el sistema    |
| fecha_movimiento  | DATE         |             | Fecha en que se realizó el movimiento |

## 13. pagos_siniestros
Almacena los pagos realizados para resolver los siniestros.

| Columna           | Tipo de Dato | Restricción | Descripción                        |
|-------------------|--------------|-------------|-----------------------------------|
| id                | VARCHAR      | PK          | Identificador único del pago       |
| id_siniestro      | VARCHAR      | FK          | Referencia al siniestro            |
| importe           | DECIMAL      |             | Monto del pago                     |
| fecha_pago        | DATE         |             | Fecha en que se realizó el pago    |
