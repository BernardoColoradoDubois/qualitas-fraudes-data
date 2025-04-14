from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.incisos_polizas.dto import IncisosPolizasDateRange

class LoadIncisoPolizas:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: IncisosPolizasDateRange):
  
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_INCISOS_POLIZAS` WHERE CAST(FECHA_CARGA AS DATE) BETWEEN '{init_date}' AND '{final_date}';", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_INCISOS_POLIZAS",
      schema="INSUMOS",
      table="DM_INCISOS_POLIZAS"
    ) 
      
    return response