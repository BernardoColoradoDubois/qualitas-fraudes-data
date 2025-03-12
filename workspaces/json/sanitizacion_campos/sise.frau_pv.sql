SELECT
    REPLACE(REPLACE(REPLACE(FRAU_PV, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FRAU_PV,
    REPLACE(REPLACE(REPLACE(COD_ASEG, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_ASEG,
    REPLACE(REPLACE(REPLACE(FEC_EMI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_EMI,
    REPLACE(REPLACE(REPLACE(VIG_DESDE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS VIG_DESDE,
    REPLACE(REPLACE(REPLACE(VIG_HASTA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS VIG_HASTA,
    REPLACE(REPLACE(REPLACE(RENOVADA_POR, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RENOVADA_POR,
    REPLACE(REPLACE(REPLACE(RENUEVA_A, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RENUEVA_A,
    REPLACE(REPLACE(REPLACE(COD_MONEDA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_MONEDA,
    REPLACE(REPLACE(REPLACE(COD_FACTURACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_FACTURACION,
    REPLACE(REPLACE(REPLACE(COD_EMISION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_EMISION,
    REPLACE(REPLACE(REPLACE(PORC_RF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PORC_RF,
    REPLACE(REPLACE(REPLACE(PORC_IVA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PORC_IVA,
    REPLACE(REPLACE(REPLACE(PORC_BONIF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PORC_BONIF,
    REPLACE(REPLACE(REPLACE(COD_CAMBIO_DP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_CAMBIO_DP,
    REPLACE(REPLACE(REPLACE(RSR, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RSR,
    REPLACE(REPLACE(REPLACE(AAMM_TARIFA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AAMM_TARIFA,
    REPLACE(REPLACE(REPLACE(AAMM_VALORES, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AAMM_VALORES,
    REPLACE(REPLACE(REPLACE(AAMM_DERPOL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AAMM_DERPOL,
    REPLACE(REPLACE(REPLACE(SUMA_ASEGURADA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SUMA_ASEGURADA,
    REPLACE(REPLACE(REPLACE(PRIMA_NETA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PRIMA_NETA,
    REPLACE(REPLACE(REPLACE(DER_POL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DER_POL,
    REPLACE(REPLACE(REPLACE(REC_FIN, CHR(10), ''), CHR(13), ''), CHR(9), '') AS REC_FIN,
    REPLACE(REPLACE(REPLACE(IVA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS IVA,
    REPLACE(REPLACE(REPLACE(BONIFICACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BONIFICACION,
    REPLACE(REPLACE(REPLACE(BONIF_RF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BONIF_RF,
    REPLACE(REPLACE(REPLACE(BONIF_TEC, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BONIF_TEC,
    REPLACE(REPLACE(REPLACE(PMA_ENDO_RF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PMA_ENDO_RF,
    REPLACE(REPLACE(REPLACE(PRIMA_TOTAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PRIMA_TOTAL,
    REPLACE(REPLACE(REPLACE(COD_PLAN_PAGO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_PLAN_PAGO,
    REPLACE(REPLACE(REPLACE(COD_PRODUCTO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_PRODUCTO,
    REPLACE(REPLACE(REPLACE(AGENTE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AGENTE,
    REPLACE(REPLACE(REPLACE(PROMOTOR, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PROMOTOR,
    REPLACE(REPLACE(REPLACE(COMIS_NORMAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COMIS_NORMAL,
    REPLACE(REPLACE(REPLACE(COMIS_RF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COMIS_RF,
    REPLACE(REPLACE(REPLACE(COMIS_TEC, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COMIS_TEC,
    REPLACE(REPLACE(REPLACE(COTIZACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COTIZACION,
    REPLACE(REPLACE(REPLACE(ENDO_QUE_MODIF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ENDO_QUE_MODIF,
    REPLACE(REPLACE(REPLACE(COD_CONDUCTO_PAGO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_CONDUCTO_PAGO,
    REPLACE(REPLACE(REPLACE(ACREEDOR_PREND, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ACREEDOR_PREND,
    REPLACE(REPLACE(REPLACE(MARCA_CANC_AUT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_CANC_AUT,
    REPLACE(REPLACE(REPLACE(COD_COBZA_COAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_COBZA_COAS,
    REPLACE(REPLACE(REPLACE(FEC_AMORTIZACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_AMORTIZACION,
    REPLACE(REPLACE(REPLACE(FEC_REHABILITACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_REHABILITACION,
    REPLACE(REPLACE(REPLACE(COD_OFIC, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_OFIC,
    REPLACE(REPLACE(REPLACE(FEC_PROCESO_SISE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_PROCESO_SISE,
    REPLACE(REPLACE(REPLACE(HORA_INI_CARGA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS HORA_INI_CARGA,
    REPLACE(REPLACE(REPLACE(HORA_FIN_CARGA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS HORA_FIN_CARGA,
    REPLACE(REPLACE(REPLACE(COD_OPER, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_OPER,
    REPLACE(REPLACE(REPLACE(POL_END_COAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POL_END_COAS,
    REPLACE(REPLACE(REPLACE(COMPANIA_PILOTO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COMPANIA_PILOTO,
    REPLACE(REPLACE(REPLACE(CIAS_PARTICIPANTES, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CIAS_PARTICIPANTES,
    REPLACE(REPLACE(REPLACE(PART_CIAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PART_CIAS,
    REPLACE(REPLACE(REPLACE(PART_CIAS_PN, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PART_CIAS_PN,
    REPLACE(REPLACE(REPLACE(COD_CANCELACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_CANCELACION,
    REPLACE(REPLACE(REPLACE(REASEGURO_AUT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS REASEGURO_AUT,
    REPLACE(REPLACE(REPLACE(TURISTA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS TURISTA,
    REPLACE(REPLACE(REPLACE(CAMBIO_MONEDA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CAMBIO_MONEDA,
    REPLACE(REPLACE(REPLACE(USUARIO_EMITE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS USUARIO_EMITE,
    REPLACE(REPLACE(REPLACE(IVA_RCP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS IVA_RCP,
    REPLACE(REPLACE(REPLACE(BON_NORM_RCP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BON_NORM_RCP,
    REPLACE(REPLACE(REPLACE(REMESA_DERECHO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS REMESA_DERECHO,
    REPLACE(REPLACE(REPLACE(AUT_BONIF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_BONIF,
    REPLACE(REPLACE(REPLACE(AUT_TARIFA_MANUAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_TARIFA_MANUAL,
    REPLACE(REPLACE(REPLACE(DESCTO_X_ZONA_AUTOS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DESCTO_X_ZONA_AUTOS,
    REPLACE(REPLACE(REPLACE(DESCTO_X_ZONA_CAMIONES, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DESCTO_X_ZONA_CAMIONES,
    REPLACE(REPLACE(REPLACE(PRORRATEO_DERECHOS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PRORRATEO_DERECHOS,
    REPLACE(REPLACE(REPLACE(AUT_PRORR_DERECHO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_PRORR_DERECHO,
    REPLACE(REPLACE(REPLACE(AUT_EE_Y_ADAP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EE_Y_ADAP,
    REPLACE(REPLACE(REPLACE(AUT_TAR_EE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_TAR_EE,
    REPLACE(REPLACE(REPLACE(RENOVACION_AUTOMATICA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RENOVACION_AUTOMATICA,
    REPLACE(REPLACE(REPLACE(FLOT_POL_IND, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FLOT_POL_IND,
    REPLACE(REPLACE(REPLACE(AUT_BONIF_RENT_AUT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_BONIF_RENT_AUT,
    REPLACE(REPLACE(REPLACE(CONDICIONES_VIGENTES, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CONDICIONES_VIGENTES,
    REPLACE(REPLACE(REPLACE(AUT_RECARGO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_RECARGO,
    REPLACE(REPLACE(REPLACE(AUT_REHAB_MAS60, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_REHAB_MAS60,
    REPLACE(REPLACE(REPLACE(AUT_ENDO_POL_VENC, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_ENDO_POL_VENC,
    REPLACE(REPLACE(REPLACE(FORMATO_RECIBO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FORMATO_RECIBO,
    REPLACE(REPLACE(REPLACE(AUT_TARIFA_ESP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_TARIFA_ESP,
    REPLACE(REPLACE(REPLACE(DIAS_PER_GRACIA_AGTE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DIAS_PER_GRACIA_AGTE,
    REPLACE(REPLACE(REPLACE(PORC_DESC_PER_GRACIA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PORC_DESC_PER_GRACIA,
    REPLACE(REPLACE(REPLACE(AUT_POL_TURISTA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_POL_TURISTA,
    REPLACE(REPLACE(REPLACE(NUM_VEH_TUR, CHR(10), ''), CHR(13), ''), CHR(9), '') AS NUM_VEH_TUR,
    REPLACE(REPLACE(REPLACE(PMA_ASIST_TURISTA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PMA_ASIST_TURISTA,
    REPLACE(REPLACE(REPLACE(POLITICA_BONO_AGENTE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POLITICA_BONO_AGENTE,
    REPLACE(REPLACE(REPLACE(POLITICA_BONO_PROM, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POLITICA_BONO_PROM,
    REPLACE(REPLACE(REPLACE(AUT_MODIF_BONO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_MODIF_BONO,
    REPLACE(REPLACE(REPLACE(AUT_REUTILIZAR_COTIZACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_REUTILIZAR_COTIZACION,
    REPLACE(REPLACE(REPLACE(AUT_CAMBIAR_ASEG_ENDO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_CAMBIAR_ASEG_ENDO,
    REPLACE(REPLACE(REPLACE(MARCA_RC_EXTRANJERO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_RC_EXTRANJERO,
    REPLACE(REPLACE(REPLACE(COD_AGENCIA1, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_AGENCIA1,
    REPLACE(REPLACE(REPLACE(DED_ADMV, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DED_ADMV,
    REPLACE(REPLACE(REPLACE(AUT_PT_RCBOS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_PT_RCBOS,
    REPLACE(REPLACE(REPLACE(AUT_POL_CON_DED_ADMVO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_POL_CON_DED_ADMVO,
    REPLACE(REPLACE(REPLACE(AUT_PAGO_MENS_TRIM_BUSES, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_PAGO_MENS_TRIM_BUSES,
    REPLACE(REPLACE(REPLACE(AUT_ELIM_ASISTSAT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_ELIM_ASISTSAT,
    REPLACE(REPLACE(REPLACE(PLAN_PISO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PLAN_PISO,
    REPLACE(REPLACE(REPLACE(AUT_TAR_MANUAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_TAR_MANUAL,
    REPLACE(REPLACE(REPLACE(COD_AGENCIA2, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_AGENCIA2,
    REPLACE(REPLACE(REPLACE(UDI_AGENCIA2, CHR(10), ''), CHR(13), ''), CHR(9), '') AS UDI_AGENCIA2,
    REPLACE(REPLACE(REPLACE(AUT_TARIF_DIF_COTIZACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_TARIF_DIF_COTIZACION,
    REPLACE(REPLACE(REPLACE(DER_POL_1ER_RCBO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DER_POL_1ER_RCBO,
    REPLACE(REPLACE(REPLACE(USUARIO_MARCA_REC_CANC, CHR(10), ''), CHR(13), ''), CHR(9), '') AS USUARIO_MARCA_REC_CANC,
    REPLACE(REPLACE(REPLACE(AUT_MOD_COBER_EE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_MOD_COBER_EE,
    REPLACE(REPLACE(REPLACE(AUT_CAMBIO_DP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_CAMBIO_DP,
    REPLACE(REPLACE(REPLACE(AUT_RENOVACION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_RENOVACION,
    REPLACE(REPLACE(REPLACE(AUT_AJUSTE_AUTOM, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_AJUSTE_AUTOM,
    REPLACE(REPLACE(REPLACE(POL_COMP_AGENCIAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POL_COMP_AGENCIAS,
    REPLACE(REPLACE(REPLACE(AUT_MOD_COM_NORM, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_MOD_COM_NORM,
    REPLACE(REPLACE(REPLACE(AUT_MOD_COM_X_RECARGO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_MOD_COM_X_RECARGO,
    REPLACE(REPLACE(REPLACE(AUT_CAMBIO_COD_ASEGURADO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_CAMBIO_COD_ASEGURADO,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_PAQ_FAM, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_PAQ_FAM,
    REPLACE(REPLACE(REPLACE(AUT_AMPARAR_SERIES_DUP_C_AGTES, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_AMPARAR_SERIES_DUP_C_AGTES,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_VEH_FRONT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_VEH_FRONT,
    REPLACE(REPLACE(REPLACE(NUM_ECOLOGICO_OPL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS NUM_ECOLOGICO_OPL,
    REPLACE(REPLACE(REPLACE(LEYENDA_DED_ADMVO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS LEYENDA_DED_ADMVO,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_MOD_ANT_CON_TAR_NO_PERM, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_MOD_ANT_CON_TAR_NO_PERM,
    REPLACE(REPLACE(REPLACE(FECHA_CANCELACION_AUT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FECHA_CANCELACION_AUT,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_TAR_NO_VIG, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_TAR_NO_VIG,
    REPLACE(REPLACE(REPLACE(EXCLUSION_DIA_BISIESTO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS EXCLUSION_DIA_BISIESTO,
    REPLACE(REPLACE(REPLACE(VALIDACION_VEH_NO_ROBADO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS VALIDACION_VEH_NO_ROBADO,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_SERVPUB_VIG_183DIA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_SERVPUB_VIG_183DIA,
    REPLACE(REPLACE(REPLACE(AUT_AMP_VEH_MENOR25_ANIOS_RCPAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_AMP_VEH_MENOR25_ANIOS_RCPAS,
    REPLACE(REPLACE(REPLACE(VIG_GARANTIA_FABRICANTE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS VIG_GARANTIA_FABRICANTE,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_ASEG_MARCADO_ART140, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_ASEG_MARCADO_ART140,
    REPLACE(REPLACE(REPLACE(COD_AGENCIA3, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_AGENCIA3,
    REPLACE(REPLACE(REPLACE(UDI_AGENCIA3, CHR(10), ''), CHR(13), ''), CHR(9), '') AS UDI_AGENCIA3,
    REPLACE(REPLACE(REPLACE(SUBRAMO_DESCUENTO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SUBRAMO_DESCUENTO,
    REPLACE(REPLACE(REPLACE(POLIZA_A_MSI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POLIZA_A_MSI,
    REPLACE(REPLACE(REPLACE(FEC_CREACION_ENDO_X_CFP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_CREACION_ENDO_X_CFP,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_SERIES_CON_PT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_SERIES_CON_PT,
    REPLACE(REPLACE(REPLACE(NUM_INCISOS_POL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS NUM_INCISOS_POL,
    REPLACE(REPLACE(REPLACE(SA_VEH_NO_INCL_BONIF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SA_VEH_NO_INCL_BONIF,
    REPLACE(REPLACE(REPLACE(AUT_DED_ENCONTRACK, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_DED_ENCONTRACK,
    REPLACE(REPLACE(REPLACE(CP_ASEGURADO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CP_ASEGURADO,
    REPLACE(REPLACE(REPLACE(CP_ALTERNO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CP_ALTERNO,
    REPLACE(REPLACE(REPLACE(USUARIO_MODIFICA_CP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS USUARIO_MODIFICA_CP,
    REPLACE(REPLACE(REPLACE(COD_EDO_Y_GPO_CP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COD_EDO_Y_GPO_CP,
    REPLACE(REPLACE(REPLACE(TIPO_POL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS TIPO_POL,
    REPLACE(REPLACE(REPLACE(CTRL_DIAS_PER_GRACIA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS CTRL_DIAS_PER_GRACIA,
    REPLACE(REPLACE(REPLACE(AUT_DIAS_PER_GRACIA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_DIAS_PER_GRACIA,
    REPLACE(REPLACE(REPLACE(AUT_MODIF_CP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_MODIF_CP,
    REPLACE(REPLACE(REPLACE(INCLUYE_ROBO_VALOR_TOTAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS INCLUYE_ROBO_VALOR_TOTAL,
    REPLACE(REPLACE(REPLACE(MARCA_EMISION_EN_SISE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_EMISION_EN_SISE,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_CON_PEPS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_CON_PEPS,
    REPLACE(REPLACE(REPLACE(IMPRESION_CERT_RC_OBL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS IMPRESION_CERT_RC_OBL,
    REPLACE(REPLACE(REPLACE(COBR_EMITIDAS_EN_INCISOS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COBR_EMITIDAS_EN_INCISOS,
    REPLACE(REPLACE(REPLACE(AUT_CANCELAR_POL_RC_OBL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_CANCELAR_POL_RC_OBL,
    REPLACE(REPLACE(REPLACE(COB_AFECTADAS_POR_DED_ADMVO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS COB_AFECTADAS_POR_DED_ADMVO,
    REPLACE(REPLACE(REPLACE(MARCA_POL_CARGA_OPL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_POL_CARGA_OPL,
    REPLACE(REPLACE(REPLACE(FEC_CARGA_POL_OPL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_CARGA_POL_OPL,
    REPLACE(REPLACE(REPLACE(PMA_NETA_X_INCISO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PMA_NETA_X_INCISO,
    REPLACE(REPLACE(REPLACE(PMA_SIN_BT_X_INCISO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PMA_SIN_BT_X_INCISO,
    REPLACE(REPLACE(REPLACE(PMA_SIN_ACUMULADO_X_INCISO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PMA_SIN_ACUMULADO_X_INCISO,
    REPLACE(REPLACE(REPLACE(PMA_NETA_BT_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PMA_NETA_BT_X_INCI,
    REPLACE(REPLACE(REPLACE(SUBRAMO_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SUBRAMO_X_INCI,
    REPLACE(REPLACE(REPLACE(DERPOL_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DERPOL_X_INCI,
    REPLACE(REPLACE(REPLACE(REC_FIN_X_INCISO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS REC_FIN_X_INCISO,
    REPLACE(REPLACE(REPLACE(IVA_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS IVA_X_INCI,
    REPLACE(REPLACE(REPLACE(TOTAL_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS TOTAL_X_INCI,
    REPLACE(REPLACE(REPLACE(BT_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BT_X_INCI,
    REPLACE(REPLACE(REPLACE(BN_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BN_X_INCI,
    REPLACE(REPLACE(REPLACE(BR_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BR_X_INCI,
    REPLACE(REPLACE(REPLACE(METODO_PAGO_OPL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS METODO_PAGO_OPL,
    REPLACE(REPLACE(REPLACE(SW_VERIFICA_AGT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SW_VERIFICA_AGT,
    REPLACE(REPLACE(REPLACE(MARCA_AGTE_CUES_DIRECTA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_AGTE_CUES_DIRECTA,
    REPLACE(REPLACE(REPLACE(AUT_SERIE_APP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_SERIE_APP,
    REPLACE(REPLACE(REPLACE(MARCA_POL_DEPURADA_HIST, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_POL_DEPURADA_HIST,
    REPLACE(REPLACE(REPLACE(TIPO_NEGOCIO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS TIPO_NEGOCIO,
    REPLACE(REPLACE(REPLACE(HISTORIAL_ADEG_EN_PLIZA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS HISTORIAL_ADEG_EN_PLIZA,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_PERSONA_SUJETA_A_REVISION, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_PERSONA_SUJETA_A_REVISION,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_PERSONA_NO_ASEG, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_PERSONA_NO_ASEG,
    REPLACE(REPLACE(REPLACE(BLOQUEO_POLIZAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BLOQUEO_POLIZAS,
    REPLACE(REPLACE(REPLACE(POL_EMITIDA_VIA_COBZA_DELEGADA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POL_EMITIDA_VIA_COBZA_DELEGADA,
    REPLACE(REPLACE(REPLACE(POLIZA_A_REEXPEDIR, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POLIZA_A_REEXPEDIR,
    REPLACE(REPLACE(REPLACE(RFC_NORM_ASEG_UNICO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RFC_NORM_ASEG_UNICO,
    REPLACE(REPLACE(REPLACE(MARCA_PMA_INSUFICIENTE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_PMA_INSUFICIENTE,
    REPLACE(REPLACE(REPLACE(ENDOSO_CFP, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ENDOSO_CFP,
    REPLACE(REPLACE(REPLACE(MARCA_CAMBIO_FP_SIN_RF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_CAMBIO_FP_SIN_RF,
    REPLACE(REPLACE(REPLACE(MARCA_POL_RC_CONTRACTUAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_POL_RC_CONTRACTUAL,
    REPLACE(REPLACE(REPLACE(POL_REFER_RC_CONTRAT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS POL_REFER_RC_CONTRAT,
    REPLACE(REPLACE(REPLACE(ENDO_REFER_RC_CONTRACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ENDO_REFER_RC_CONTRACT,
    REPLACE(REPLACE(REPLACE(INCI_REFER_RC_CONTRACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS INCI_REFER_RC_CONTRACT,
    REPLACE(REPLACE(REPLACE(FEC_EMI_REFER_RC_CONTRACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FEC_EMI_REFER_RC_CONTRACT,
    REPLACE(REPLACE(REPLACE(DESDE_REFER_RC_CONTRACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DESDE_REFER_RC_CONTRACT,
    REPLACE(REPLACE(REPLACE(HASTA_REFER_RC_CONTRACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS HASTA_REFER_RC_CONTRACT,
    REPLACE(REPLACE(REPLACE(TIPO_POL_RC_CONTRACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS TIPO_POL_RC_CONTRACT,
    REPLACE(REPLACE(REPLACE(TIPO_DOC_RC_CONTRACT, CHR(10), ''), CHR(13), ''), CHR(9), '') AS TIPO_DOC_RC_CONTRACT,
    REPLACE(REPLACE(REPLACE(RCBOS_SUBSEC_BON_TEC_X_INCI, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RCBOS_SUBSEC_BON_TEC_X_INCI,
    REPLACE(REPLACE(REPLACE(RCBOS_SUBSEC_BON_TEC_X_INCI_2, CHR(10), ''), CHR(13), ''), CHR(9), '') AS RCBOS_SUBSEC_BON_TEC_X_INCI_2,
    REPLACE(REPLACE(REPLACE(SUBTOTAL, CHR(10), ''), CHR(13), ''), CHR(9), '') AS SUBTOTAL,
    REPLACE(REPLACE(REPLACE(ASEG_UNICO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS ASEG_UNICO,
    REPLACE(REPLACE(REPLACE(AUT_EMITIR_ASEG_ALTO_RIESGO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_EMITIR_ASEG_ALTO_RIESGO,
    REPLACE(REPLACE(REPLACE(AUT_SERIES_CANCELADAS, CHR(10), ''), CHR(13), ''), CHR(9), '') AS AUT_SERIES_CANCELADAS,
    REPLACE(REPLACE(REPLACE(FECHA_PROCESO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS FECHA_PROCESO,
    REPLACE(REPLACE(REPLACE(MARCA_POL_AUTOEXPEDIBLE, CHR(10), ''), CHR(13), ''), CHR(9), '') AS MARCA_POL_AUTOEXPEDIBLE,
    REPLACE(REPLACE(REPLACE(PORC_BONRF, CHR(10), ''), CHR(13), ''), CHR(9), '') AS PORC_BONRF,
    REPLACE(REPLACE(REPLACE(BM, CHR(10), ''), CHR(13), ''), CHR(9), '') AS BM,
    REPLACE(REPLACE(REPLACE(DESC_PER_GRACIA, CHR(10), ''), CHR(13), ''), CHR(9), '') AS DESC_PER_GRACIA,
    REPLACE(REPLACE(REPLACE(TIPO_ENDO, CHR(10), ''), CHR(13), ''), CHR(9), '') AS TIPO_ENDO,
    CONSEC
FROM 
    SASFRAAP.FRAUD_PV
