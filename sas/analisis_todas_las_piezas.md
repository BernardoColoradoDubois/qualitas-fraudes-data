# Análisis del Procedimiento SAS SQL - Tablas de Entrada y Flujo del Proceso

## 📋 TABLAS DE ENTRADA NECESARIAS

### **Librería VALPROD (Valuación y Producción)**
1. **VALPROD.CAUSACAMBIOVALE** - Catálogo de causas de cambio de vale
2. **VALPROD.COSTO** - Costos de refacciones (filtrado por CONCEPTO = 'REF')
3. **VALPROD.COMPLEMENTO** - Complementos de refacciones (filtrado por CONCEPTO = 'REF')
4. **VALPROD.VISTA_VALE** - Vista principal de vales
5. **VALPROD.VALEESTATUS** - Catálogo de estatus de vales
6. **VALPROD.VALEHISTORICO** - Historial detallado de vales y piezas
7. **VALPROD.RELACIONCDR_SICDR** - Relación entre talleres CDR y SICDR
8. **VALPROD.SUPERVISORINTEGRAL** - Supervisores integrales por región
9. **VALPROD.DATOSGENERALES** - Datos generales de expedientes
10. **VALPROD.FECHAS** - Fechas importantes de valuación
11. **VALPROD.VALUACION** - Tipos de valuación
12. **VALPROD.CERCO** - Catálogo de cercos (entidades)
13. **VALPROD.ESTADO** - Estados y regiones geográficas
14. **VALPROD.VALUADOR** - Información de valuadores
15. **VALPROD.CATEGORIA** - Categorías de valuadores
16. **VALPROD.DATOSVEHICULO** - Datos del vehículo
17. **VALPROD.MARCA** - Marcas de vehículos
18. **VALPROD.ESTATUS** - Estatus de expedientes
19. **VALPROD.ESTATUSEXPEDIENTES** - Catálogo de estatus de expedientes
20. **VALPROD.TALLERES** - Información de talleres CDR
21. **VALPROD.PROVEEDOR** - Catálogo de proveedores
22. **VALPROD.TIPOTOT** - Tipos TOT de proveedores
23. **VALPROD.EXPEDIENTE** - Tabla principal de expedientes
24. **VALPROD.ENVIOHISTORICO** - Historial de envíos y autorizaciones
25. **VALPROD.HISTORICOTERMINOENTREGA** - Historial de términos y entregas

### **Librería BSC_SINI (Business Scorecard Siniestros)**
26. **BSC_SINI.Prestadores** - Prestadores con marca y tipo
27. **BSC_SINI.TESTADO_BSC** - Estados y poblaciones comerciales
28. **BSC_SINI.tipoProveedor** - Tipos de proveedor

### **Librería CONVENB (Convenios Base)**
29. **CONVENB.USUARIOHOMOLOGADO** - Homologación de usuarios

---

## 🔄 FLUJO DEL PROCEDIMIENTO

### **FASE 1: EXTRACCIÓN DE CATÁLOGOS** 
📥 *Extrae todas las tablas maestras y catálogos necesarios*
- Causas de cambio, estatus, tipos de valuación
- Información de proveedores, talleres, valuadores
- Datos de vehículos, marcas, estados

### **FASE 2: PROCESAMIENTO DE FECHAS DE AUTORIZACIÓN**
📅 *Determina la fecha mínima de autorización por expediente*
- Extrae fechas de `ENVIOHISTORICO` (≥ 01ene2019)
- Extrae fechas de `HISTORICOTERMINOENTREGA` con tipos 'AUTORIZA VAL'
- Combina ambas fuentes y calcula el mínimo por expediente
- **Resultado:** `QUERY_FOR_ENVIOHISTORICO` (expedientes con fecha mín. autorización)

### **FASE 3: CONSOLIDACIÓN DE INFORMACIÓN DE PRESTADORES**
🏢 *Enriquece información de proveedores con datos externos*
- Une prestadores con tipos de proveedor y poblaciones comerciales
- Cruza proveedores VALPROD con información BSC_SINI
- **Resultado:** `QUERY_FOR_PROVEEDOR` (proveedores enriquecidos)

### **FASE 4: PREPARACIÓN DE INFORMACIÓN BASE**
🔧 *Prepara las tablas base para el concentrado principal*
- Procesa talleres con columnas adicionales
- Une estatus de expedientes con sus descripciones
- Combina datos de vehículos con marcas
- Clasifica valuadores por categoría y tipo (equipo pesado/autos)
- Identifica analistas CDR por región

### **FASE 5: PRIMER CONCENTRADO**
📊 *Crea el concentrado principal de expedientes*
- Une expedientes con datos generales, fechas, valuación
- Agrega información de talleres, valuadores y cercos
- Limpia y formatea campos (ej: ESTATUSVALUACION)
- Calcula gerencia de valuación por región geográfica
- **Resultado:** `CONCENTRADO` con información completa de expedientes

### **FASE 6: PROCESAMIENTO DE VALES Y PIEZAS**
🔩 *Procesa el detalle de vales y refacciones*
- Une vales con proveedores y estatus
- Filtra histórico de vales (FECHAEXPEDICION ≥ 01ene2023)
- Clasifica origen de piezas (ORIGINAL, ALTERNATIVO, TOT, etc.)
- Une con costos y complementos según tipo
- Agrega causas de cambio de vale

### **FASE 7: CÁLCULO DE TIEMPOS DE ENTREGA**
⏱️ *Calcula métricas de tiempo por pieza*
- Calcula tiempo de entrega por expediente-proveedor-pieza
- Usa `INTCK('DTWEEKDAY1W')` para días hábiles
- Calcula tiempos individuales por vale
- **Métricas:** TIEMPOENTREGA, TIEMPOENTREGA1, TIEMPORECEPCION

### **FASE 8: CLASIFICACIÓN DE ASIGNACIONES**
👥 *Clasifica el tipo de asignación de cada pieza*
- **AUTOMÁTICO:** Usuario = 'AUT'
- **MANUAL CON ALERTA:** Usuario contiene 'AUT /'
- **MANUAL COMPRAS:** Usuarios específicos de compras
- **MANUAL SEGUIMIENTO:** Usuarios ASR, supervisores
- **DIRECTA:** Usuarios numéricos o 'Valuador'
- Identifica área responsable y piezas autosurtido

### **FASE 9: HOMOLOGACIÓN DE USUARIOS**
🔄 *Estandariza nombres de usuarios*
- Cruza con tabla de homologación `CONVENB.USUARIOHOMOLOGADO`
- Aplica reglas específicas para casos especiales
- Maneja usuarios automáticos y valuadores

### **FASE 10: RESULTADO FINAL**
✅ *Genera la tabla final filtrada*
- Filtra por `FECVALUACION >= '1Jan2024:0:0:0'dt`
- Guarda en `INDBASE.TODASLASPIEZAS`
- **87 campos finales** con información completa

---

## 📈 MÉTRICAS Y DIMENSIONES PRINCIPALES

### **Dimensiones Clave:**
- **Expediente:** IDEXPEDIENTE, NUMVALUACION, ejercicio, reporte
- **Vehículo:** Marca, tipo, modelo, serie
- **Taller:** Clave, nombre, tipo CDR, región, gerencia
- **Proveedor:** Código, nombre, tipo, población comercial
- **Pieza:** Número parte, referencia, descripción, origen
- **Usuario:** Original, homologado, tipo asignación, área

### **Métricas Calculadas:**
- **Tiempos:** Entrega, recepción (en días hábiles)
- **Indicadores:** Pieza entregada, vehículo terminado, autosurtido
- **Clasificaciones:** Tipo asignación, área responsable
- **Montos:** Monto pieza, monto convenio

### **Filtros Aplicados:**
- Expedientes con autorización ≥ 01ene2019
- Vales con expedición ≥ 01ene2023  
- Valuaciones ≥ 01ene2024
- Solo refacciones (CONCEPTO = 'REF')
- Solo registros con IDCOSTO válido

---

## 🎯 OBJETIVO DEL PROCEDIMIENTO

El procedimiento genera un **dataset integrado** que permite analizar:
- ✅ Desempeño de proveedores y talleres
- ✅ Tiempos de entrega de refacciones
- ✅ Eficiencia de procesos de asignación
- ✅ Métricas operativas por región/gerencia
- ✅ Seguimiento completo del ciclo de vida de piezas