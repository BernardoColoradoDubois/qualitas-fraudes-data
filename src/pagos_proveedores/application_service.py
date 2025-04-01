from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadPagosProveedores:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_PROVEEDORES` ORDER BY ID LIMIT 10000;", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_PROVEEDORES",
      schema="INSUMOS",
      table="DM_PAGOS_PROVEEDORES"
    )    
    
    return response