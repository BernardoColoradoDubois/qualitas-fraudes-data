from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.polizas_vigentes.dto import PolizasVigentesDateRange

class LoadPolizasVigentes:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: PolizasVigentesDateRange):
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_POLIZAS_VIGENTES` WHERE CAST(FECHA_PROCESO AS DATE) BETWEEN '{init_date}' AND '{final_date}';", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_POLIZAS_VIGENTES",
      schema="INSUMOS",
      table="DM_POLIZAS_VIGENTES"
    )  
    
    return response