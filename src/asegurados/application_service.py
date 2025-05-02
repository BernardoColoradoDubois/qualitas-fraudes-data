from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadAsegurados:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_ASEGURADOS` LIMIT 50000;", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_ASEGURADOS",
      schema="INSUMOS",
      table="DM_ASEGURADOS"
    )    
    
    return response