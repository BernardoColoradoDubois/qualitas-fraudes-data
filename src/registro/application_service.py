from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadRegistro:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_REGISTRO` ORDER BY ID_SINIESTRO;", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_REGISTRO",
      schema="INSUMOS",
      table="DM_REGISTRO"
    )    
    
    return response