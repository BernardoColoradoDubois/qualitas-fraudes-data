CREATE OR REPLACE TABLE `{{task.params.DEST_PROJECT_ID}}.{{task.params.DEST_DATASET_NAME}}.{{task.params.DEST_TABLE_NAME}}` AS
SELECT 
  -- 1. RAMO
  SAFE_CAST(SAFE_CAST(NULLIF(RAMO, '') AS FLOAT64) AS INT64) AS RAMO,
  
  -- 2. EJERCICIO
  SAFE_CAST(SAFE_CAST(NULLIF(EJERCICIO, '') AS FLOAT64) AS INT64) AS EJERCICIO,
  
  -- 3. SINIESTRO
  SAFE_CAST(SAFE_CAST(NULLIF(SINIESTRO, '') AS FLOAT64) AS INT64) AS SINIESTRO,
  
  -- 4. REPORTE
  SAFE_CAST(SAFE_CAST(NULLIF(REPORTE, '') AS FLOAT64) AS INT64) AS REPORTE,
  
  -- 5. POLIZA
  SAFE_CAST(SAFE_CAST(NULLIF(POLIZA, '') AS FLOAT64) AS INT64) AS POLIZA,
  
  -- 6. ENDOSO
  SAFE_CAST(SAFE_CAST(NULLIF(ENDOSO, '') AS FLOAT64) AS INT64) AS ENDOSO,
  
  -- 7. INCISO
  SAFE_CAST(SAFE_CAST(NULLIF(INCISO, '') AS FLOAT64) AS INT64) AS INCISO,
  
  -- 8. CAUSA_SINIESTRO
  CAUSA_SINIESTRO,
  
  -- 9. FECHA_OCURRIDO (formato dd/MM/yyyy)
  CASE 
    WHEN FECHA_OCURRIDO IS NOT NULL AND FECHA_OCURRIDO != '' AND FECHA_OCURRIDO LIKE '%/%/%'
    THEN SAFE.PARSE_DATE('%d/%m/%Y', FECHA_OCURRIDO)
    ELSE NULL 
  END AS FECHA_OCURRIDO,
  
  -- 10. FECHA_DEMANDA_LEGAL (formato dd/MM/yyyy si no está vacía)
  CASE 
    WHEN FECHA_DEMANDA_LEGAL IS NOT NULL AND FECHA_DEMANDA_LEGAL != '' AND FECHA_DEMANDA_LEGAL LIKE '%/%/%'
    THEN SAFE.PARSE_DATE('%d/%m/%Y', FECHA_DEMANDA_LEGAL)
    ELSE NULL 
  END AS FECHA_DEMANDA_LEGAL,
  
  -- 11. SERIE
  SERIE,
  
  -- 12. SUMA_ASEGURADA
  SAFE_CAST(NULLIF(SUMA_ASEGURADA, '') AS FLOAT64) AS SUMA_ASEGURADA,
  
  -- 13. DEDUCIBLE_DM (extraer valor numérico de formatos como "5.00 %", "0,000")
  CASE 
    WHEN DEDUCIBLE_DM IS NOT NULL AND DEDUCIBLE_DM != '' THEN
      SAFE_CAST(
        REPLACE(
          REPLACE(
            REPLACE(DEDUCIBLE_DM, '%', ''),
            ',', '.'
          ), ' ', ''
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUCIBLE_DM,
  
  -- 14. DEDUCIBLE_RT (extraer valor numérico de formatos como "10.00 %", "4,000")
  CASE 
    WHEN DEDUCIBLE_RT IS NOT NULL AND DEDUCIBLE_RT != '' THEN
      SAFE_CAST(
        REPLACE(
          REPLACE(
            REPLACE(DEDUCIBLE_RT, '%', ''),
            ',', '.'
          ), ' ', ''
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUCIBLE_RT,
  
  -- 15. DEDUDIBLE_RC (extraer valor numérico de formatos como "0 Dias", "50 Dias", "100 Dias")
  CASE 
    WHEN DEDUDIBLE_RC IS NOT NULL AND DEDUDIBLE_RC != '' THEN
      SAFE_CAST(
        TRIM(
          REPLACE(
            REPLACE(DEDUDIBLE_RC, 'Dias', ''),
            'dias', ''
          )
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUDIBLE_RC,
  
  -- 16. DEDUCIBLE_EE (extraer valor numérico de formatos como "25.00 %")
  CASE 
    WHEN DEDUCIBLE_EE IS NOT NULL AND DEDUCIBLE_EE != '' THEN
      SAFE_CAST(
        REPLACE(
          REPLACE(
            REPLACE(DEDUCIBLE_EE, '%', ''),
            ',', '.'
          ), ' ', ''
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUCIBLE_EE,
  
  -- 17. DEDUCIBLE_RCB (extraer valor numérico si existe)
  CASE 
    WHEN DEDUCIBLE_RCB IS NOT NULL AND DEDUCIBLE_RCB != '' THEN
      SAFE_CAST(
        REPLACE(
          REPLACE(
            REPLACE(DEDUCIBLE_RCB, '%', ''),
            ',', '.'
          ), ' ', ''
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUCIBLE_RCB,
  
  -- 18. DEDUCIBLE_RCPER (extraer valor numérico si existe)
  CASE 
    WHEN DEDUCIBLE_RCPER IS NOT NULL AND DEDUCIBLE_RCPER != '' THEN
      SAFE_CAST(
        REPLACE(
          REPLACE(
            REPLACE(DEDUCIBLE_RCPER, '%', ''),
            ',', '.'
          ), ' ', ''
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUCIBLE_RCPER,
  
  -- 19. DEDUCIBLE_RCPAS (extraer valor numérico si existe)
  CASE 
    WHEN DEDUCIBLE_RCPAS IS NOT NULL AND DEDUCIBLE_RCPAS != '' THEN
      SAFE_CAST(
        REPLACE(
          REPLACE(
            REPLACE(DEDUCIBLE_RCPAS, '%', ''),
            ',', '.'
          ), ' ', ''
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUCIBLE_RCPAS,
  
  -- 20. DEDUCIBLE_RCC (extraer valor numérico si existe)
  CASE 
    WHEN DEDUCIBLE_RCC IS NOT NULL AND DEDUCIBLE_RCC != '' THEN
      SAFE_CAST(
        REPLACE(
          REPLACE(
            REPLACE(DEDUCIBLE_RCC, '%', ''),
            ',', '.'
          ), ' ', ''
        ) AS FLOAT64
      )
    ELSE NULL 
  END AS DEDUCIBLE_RCC

FROM `{{task.params.SOURCE_PROJECT_ID}}.{{task.params.SOURCE_DATASET_NAME}}.{{task.params.SOURCE_TABLE_NAME}}`