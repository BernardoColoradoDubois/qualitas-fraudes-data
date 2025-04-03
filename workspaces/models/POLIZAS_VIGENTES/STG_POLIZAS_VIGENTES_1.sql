CREATE OR REPLACE TABLE `STG_FRAUDES.STG_POLIZAS_VIGENTES_1` AS
SELECT 
  FRAU_PV
  ,SUBSTRING(FRAU_PV,0,2) AS RAMO
  ,SUBSTRING(FRAU_PV,3,10) AS POLIZA
  ,SUBSTRING(FRAU_PV,13,6) AS ENDOSO
  ,SUBSTRING(FRAU_PV,3) AS POLIZA_ENDOSO
  ,COD_ASEG
  ,CAST(CONCAT(SUBSTRING(FEC_EMI,7,4),'-',SUBSTRING(FEC_EMI,4,2),'-',SUBSTRING(FEC_EMI,1,2)) AS DATETIME) AS FEC_EMI
  ,CAST(CONCAT(SUBSTRING(VIG_DESDE,7,4),'-',SUBSTRING(VIG_DESDE,4,2),'-',SUBSTRING(VIG_DESDE,1,2)) AS DATETIME) AS VIG_DESDE
  ,CAST(CONCAT(SUBSTRING(VIG_HASTA,7,4),'-',SUBSTRING(VIG_HASTA,4,2),'-',SUBSTRING(VIG_HASTA,1,2)) AS DATETIME) AS VIG_HASTA
  ,RENOVADA_POR
  ,RENUEVA_A
  ,COD_MONEDA
  ,COD_FACTURACION
  ,COD_EMISION
  ,SAFE_CAST(PORC_RF AS NUMERIC) AS PORC_RF
  ,SAFE_CAST(PORC_IVA AS NUMERIC) AS PORC_IVA
  ,SAFE_CAST(PORC_BONIF AS NUMERIC) AS PORC_BONIF
  ,COD_CAMBIO_DP
  ,RSR
  ,SAFE_CAST(AAMM_TARIFA AS NUMERIC) AS AAMM_TARIFA
  ,SAFE_CAST(AAMM_VALORES AS NUMERIC) AS AAMM_VALORES
  ,SAFE_CAST(AAMM_DERPOL AS NUMERIC) AS AAMM_DERPOL
  ,SAFE_CAST(SUMA_ASEGURADA AS NUMERIC) AS SUMA_ASEGURADA
  ,SAFE_CAST(PRIMA_NETA AS NUMERIC) AS PRIMA_NETA
  ,SAFE_CAST(DER_POL AS NUMERIC) AS DER_POL
  ,SAFE_CAST(REC_FIN AS NUMERIC) AS REC_FIN
  ,SAFE_CAST(IVA AS NUMERIC) AS IVA
  ,SAFE_CAST(BONIFICACION AS NUMERIC) AS BONIFICACION
  ,SAFE_CAST(BONIF_RF AS NUMERIC) AS BONIF_RF
  ,SAFE_CAST(BONIF_TEC AS NUMERIC) AS BONIF_TEC
  ,SAFE_CAST(PMA_ENDO_RF AS NUMERIC) AS PMA_ENDO_RF
  ,SAFE_CAST(PRIMA_TOTAL AS NUMERIC) AS PRIMA_TOTAL
  ,COD_PLAN_PAGO
  ,COD_PRODUCTO
  ,AGENTE
  ,PROMOTOR
  ,SAFE_CAST(COMIS_NORMAL AS NUMERIC) AS COMIS_NORMAL
  ,SAFE_CAST(COMIS_RF AS NUMERIC) AS COMIS_RF
  ,SAFE_CAST(COMIS_TEC AS NUMERIC) AS COMIS_TEC
  ,SAFE_CAST(COTIZACION AS NUMERIC) AS COTIZACION
  ,ENDO_QUE_MODIF
  ,COD_CONDUCTO_PAGO
  ,ACREEDOR_PREND
  ,MARCA_CANC_AUT
  ,COD_COBZA_COAS
  ,CAST(CONCAT(SUBSTRING(FEC_AMORTIZACION,7,4),'-',SUBSTRING(FEC_AMORTIZACION,4,2),'-',SUBSTRING(FEC_AMORTIZACION,1,2)) AS DATETIME) AS FEC_AMORTIZACION
  ,CAST(CONCAT(SUBSTRING(FEC_REHABILITACION,7,4),'-',SUBSTRING(FEC_REHABILITACION,4,2),'-',SUBSTRING(FEC_REHABILITACION,1,2)) AS DATETIME) AS FEC_REHABILITACION
  ,COD_OFIC
  ,CAST(CONCAT(SUBSTRING(FEC_PROCESO_SISE,7,4),'-',SUBSTRING(FEC_PROCESO_SISE,4,2),'-',SUBSTRING(FEC_PROCESO_SISE,1,2)) AS DATETIME) AS FEC_PROCESO_SISE
  ,HORA_INI_CARGA
  ,HORA_FIN_CARGA
  ,COD_OPER
  ,POL_END_COAS
  ,COMPANIA_PILOTO
  ,CIAS_PARTICIPANTES
  ,PART_CIAS
  ,PART_CIAS_PN
  ,COD_CANCELACION
  ,REASEGURO_AUT
  ,TURISTA
  ,SAFE_CAST(CAMBIO_MONEDA AS NUMERIC) AS CAMBIO_MONEDA
  ,USUARIO_EMITE
  ,SAFE_CAST(IVA_RCP AS NUMERIC) AS IVA_RCP
  ,SAFE_CAST(BON_NORM_RCP AS NUMERIC) AS BON_NORM_RCP
  ,REMESA_DERECHO
  ,AUT_BONIF
  ,AUT_TARIFA_MANUAL
  ,DESCTO_X_ZONA_AUTOS
  ,DESCTO_X_ZONA_CAMIONES
  ,PRORRATEO_DERECHOS
  ,AUT_PRORR_DERECHO
  ,AUT_EE_Y_ADAP
  ,AUT_TAR_EE
  ,RENOVACION_AUTOMATICA
  ,FLOT_POL_IND
  ,AUT_BONIF_RENT_AUT
  ,CONDICIONES_VIGENTES
  ,AUT_RECARGO
  ,AUT_REHAB_MAS60
  ,AUT_ENDO_POL_VENC
  ,FORMATO_RECIBO
  ,AUT_TARIFA_ESP
  ,SAFE_CAST(DIAS_PER_GRACIA_AGTE AS NUMERIC) AS DIAS_PER_GRACIA_AGTE
  ,SAFE_CAST(PORC_DESC_PER_GRACIA AS NUMERIC) AS PORC_DESC_PER_GRACIA
  ,AUT_POL_TURISTA
  ,NUM_VEH_TUR
  ,SAFE_CAST(PMA_ASIST_TURISTA AS NUMERIC) AS PMA_ASIST_TURISTA
  ,POLITICA_BONO_AGENTE
  ,POLITICA_BONO_PROM
  ,AUT_MODIF_BONO
  ,AUT_REUTILIZAR_COTIZACION
  ,AUT_CAMBIAR_ASEG_ENDO
  ,MARCA_RC_EXTRANJERO
  ,COD_AGENCIA1
  ,DED_ADMV
  ,AUT_PT_RCBOS
  ,AUT_POL_CON_DED_ADMVO
  ,AUT_PAGO_MENS_TRIM_BUSES
  ,AUT_ELIM_ASISTSAT
  ,PLAN_PISO
  ,AUT_TAR_MANUAL
  ,COD_AGENCIA2
  ,UDI_AGENCIA2
  ,AUT_TARIF_DIF_COTIZACION
  ,SAFE_CAST(DER_POL_1ER_RCBO AS NUMERIC) AS DER_POL_1ER_RCBO
  ,USUARIO_MARCA_REC_CANC
  ,AUT_MOD_COBER_EE
  ,AUT_CAMBIO_DP
  ,AUT_RENOVACION
  ,AUT_AJUSTE_AUTOM
  ,SAFE_CAST(POL_COMP_AGENCIAS AS NUMERIC) AS POL_COMP_AGENCIAS
  ,AUT_MOD_COM_NORM
  ,AUT_MOD_COM_X_RECARGO
  ,AUT_CAMBIO_COD_ASEGURADO
  ,AUT_EMITIR_PAQ_FAM
  ,AUT_AMPARAR_SERIES_DUP_C_AGTES
  ,AUT_EMITIR_VEH_FRONT
  ,NUM_ECOLOGICO_OPL
  ,LEYENDA_DED_ADMVO
  ,AUT_EMITIR_MOD_ANT_CON_TAR_NO_PERM
  ,CAST(CONCAT(SUBSTRING(FECHA_CANCELACION_AUT,7,4),'-',SUBSTRING(FECHA_CANCELACION_AUT,4,2),'-',SUBSTRING(FECHA_CANCELACION_AUT,1,2)) AS DATETIME) AS FECHA_CANCELACION_AUT
  ,AUT_EMITIR_TAR_NO_VIG
  ,EXCLUSION_DIA_BISIESTO
  ,VALIDACION_VEH_NO_ROBADO
  ,AUT_EMITIR_SERVPUB_VIG_183DIA
  ,AUT_AMP_VEH_MENOR25_ANIOS_RCPAS
  ,VIG_GARANTIA_FABRICANTE
  ,AUT_EMITIR_ASEG_MARCADO_ART140
  ,COD_AGENCIA3
  ,UDI_AGENCIA3
  ,SUBRAMO_DESCUENTO
  ,POLIZA_A_MSI
  ,CAST(CONCAT(SUBSTRING(FEC_CREACION_ENDO_X_CFP,7,4),'-',SUBSTRING(FEC_CREACION_ENDO_X_CFP,4,2),'-',SUBSTRING(FEC_CREACION_ENDO_X_CFP,1,2)) AS DATETIME) AS FEC_CREACION_ENDO_X_CFP
  ,AUT_EMITIR_SERIES_CON_PT
  ,SAFE_CAST(NUM_INCISOS_POL AS INTEGER) AS NUM_INCISOS_POL
  ,SAFE_CAST(SA_VEH_NO_INCL_BONIF AS INTEGER) AS SA_VEH_NO_INCL_BONIF
  ,AUT_DED_ENCONTRACK
  ,CP_ASEGURADO
  ,CP_ALTERNO
  ,USUARIO_MODIFICA_CP
  ,COD_EDO_Y_GPO_CP
  ,TIPO_POL
  ,CTRL_DIAS_PER_GRACIA
  ,AUT_DIAS_PER_GRACIA
  ,AUT_MODIF_CP
  ,INCLUYE_ROBO_VALOR_TOTAL
  ,MARCA_EMISION_EN_SISE
  ,AUT_EMITIR_CON_PEPS
  ,IMPRESION_CERT_RC_OBL
  ,COBR_EMITIDAS_EN_INCISOS
  ,AUT_CANCELAR_POL_RC_OBL
  ,COB_AFECTADAS_POR_DED_ADMVO
  ,MARCA_POL_CARGA_OPL
  ,FEC_CARGA_POL_OPL
  ,SAFE_CAST(PMA_NETA_X_INCISO AS NUMERIC) AS PMA_NETA_X_INCISO
  ,SAFE_CAST(PMA_SIN_BT_X_INCISO AS NUMERIC) AS PMA_SIN_BT_X_INCISO
  ,SAFE_CAST(PMA_SIN_ACUMULADO_X_INCISO AS NUMERIC) AS PMA_SIN_ACUMULADO_X_INCISO
  ,SAFE_CAST(PMA_NETA_BT_X_INCI AS NUMERIC) AS PMA_NETA_BT_X_INCI
  ,SUBRAMO_X_INCI
  ,SAFE_CAST(DERPOL_X_INCI AS NUMERIC) AS DERPOL_X_INCI
  ,SAFE_CAST(REC_FIN_X_INCISO AS NUMERIC) AS REC_FIN_X_INCISO
  ,SAFE_CAST(IVA_X_INCI AS NUMERIC) AS IVA_X_INCI
  ,SAFE_CAST(TOTAL_X_INCI AS NUMERIC) AS TOTAL_X_INCI
  ,SAFE_CAST(BT_X_INCI AS NUMERIC) AS BT_X_INCI
  ,SAFE_CAST(BN_X_INCI AS NUMERIC) AS BN_X_INCI
  ,SAFE_CAST(BR_X_INCI AS NUMERIC) AS BR_X_INCI
  ,METODO_PAGO_OPL
  ,SW_VERIFICA_AGT
  ,MARCA_AGTE_CUES_DIRECTA
  ,AUT_SERIE_APP
  ,MARCA_POL_DEPURADA_HIST
  ,TIPO_NEGOCIO
  ,HISTORIAL_ADEG_EN_PLIZA
  ,AUT_EMITIR_PERSONA_SUJETA_A_REVISION
  ,AUT_EMITIR_PERSONA_NO_ASEG
  ,BLOQUEO_POLIZAS
  ,POL_EMITIDA_VIA_COBZA_DELEGADA
  ,POLIZA_A_REEXPEDIR
  ,RFC_NORM_ASEG_UNICO
  ,MARCA_PMA_INSUFICIENTE
  ,ENDOSO_CFP
  ,MARCA_CAMBIO_FP_SIN_RF
  ,MARCA_POL_RC_CONTRACTUAL
  ,POL_REFER_RC_CONTRAT
  ,ENDO_REFER_RC_CONTRACT
  ,INCI_REFER_RC_CONTRACT
  ,FEC_EMI_REFER_RC_CONTRACT
  ,DESDE_REFER_RC_CONTRACT
  ,HASTA_REFER_RC_CONTRACT
  ,TIPO_POL_RC_CONTRACT
  ,TIPO_DOC_RC_CONTRACT
  ,RCBOS_SUBSEC_BON_TEC_X_INCI
  ,RCBOS_SUBSEC_BON_TEC_X_INCI_2
  ,SAFE_CAST(SUBTOTAL AS NUMERIC) AS SUBTOTAL
  ,ASEG_UNICO
  ,AUT_EMITIR_ASEG_ALTO_RIESGO
  ,AUT_SERIES_CANCELADAS
  ,CAST(CONCAT(SUBSTRING(FECHA_PROCESO,7,4),'-',SUBSTRING(FECHA_PROCESO,4,2),'-',SUBSTRING(FECHA_PROCESO,1,2)) AS DATETIME) AS FECHA_PROCESO
  ,MARCA_POL_AUTOEXPEDIBLE
  ,PORC_BONRF
  ,BM
  ,DESC_PER_GRACIA
  ,TIPO_ENDO
  ,CONSEC
  ,FECHA_CARGA
FROM `sample_landing_sise.fraud_pv`
WHERE RENOVADA_POR IS NOT NULL AND FRAU_PV NOT IN  ('041030264400000000','041030266300000000','041030264400000000')