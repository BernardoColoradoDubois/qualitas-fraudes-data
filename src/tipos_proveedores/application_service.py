from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadTiposProveedores:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    
    response = self.bigquery_to_oracle.run(
      extraction_query="SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_TIPOS_PROVEEDORES`;", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_TIPOS_PROVEEDORES;",
      schema="INSUMOS",
      table="DM_TIPOS_PROVEEDORES"
    )    
    
    return response