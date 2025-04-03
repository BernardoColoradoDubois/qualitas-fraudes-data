from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.pagos_polizas.dto import PagosPolizasDateRange

class LoadPagosPolizas:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: PagosPolizasDateRange):
    
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_POLIZAS` WHERE CAST(FECHA_CARGA AS DATE) BETWEEN '{init_date}' AND '{final_date}';", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_POLIZAS",
      schema="INSUMOS",
      table="DM_PAGOS_POLIZAS"
    )   
    
    return response