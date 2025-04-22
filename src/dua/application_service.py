from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.dua.dto import DuaDateRange

class LoadDua:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: DuaDateRange):
  
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_DUA` WHERE CAST(DU_FECHA_OCURRIDO AS DATE) BETWEEN '{init_date}' AND '{final_date}' LIMIT 50000;", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_DUA",
      schema="INSUMOS",
      table="DM_DUA"
    ) 
      
    return response