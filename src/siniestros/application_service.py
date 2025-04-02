from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.siniestros.dto import SiniestrosDateRange

class LoadSiniestros:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: SiniestrosDateRange):
    
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `DM_FRAUDES.DM_SINIESTROS` WHERE CAST(FECHA_REGISTRO AS DATE) BETWEEN '{init_date}' AND '{final_date}';",  
      preload_query="TRUNCATE TABLE INSUMOS.DM_SINIESTROS",
      schema="INSUMOS",
      table="DM_SINIESTROS"
    )   

    return response