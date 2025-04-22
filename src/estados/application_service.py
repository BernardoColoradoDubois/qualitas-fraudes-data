from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadEstados:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `DM_FRAUDES.DM_ESTADOS`;", 
      preload_query="DELETE FROM INSUMOS.DM_ESTADOS WHERE 1=1 ;",
      schema="INSUMOS",
      table="DM_ESTADOS"
    )    
    
    return response