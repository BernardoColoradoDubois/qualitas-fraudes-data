from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadSiniestros:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `DM_FRAUDES.DM_SINIESTROS` WHERE CAST(FECHA_REGISTRO AS DATE) > '2025-01-01' LIMIT 10000;", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_SINIESTROS",
      schema="INSUMOS",
      table="DM_SINIESTROS"
    )   

    return response