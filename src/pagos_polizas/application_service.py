from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadPagosPolizas:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_POLIZAS` ORDER BY ID", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_POLIZAS",
      schema="INSUMOS",
      table="DM_PAGOS_POLIZAS"
    )   
    
    return response