from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadCoberturasMovimientos:
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_COBERTURAS_MOVIMIENTOS` ORDER BY ID LIMIT 10000;", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_COBERTURAS_MOVIMIENTOS",
      schema="INSUMOS",
      table="DM_COBERTURAS_MOVIMIENTOS"
    )    
    
    return response