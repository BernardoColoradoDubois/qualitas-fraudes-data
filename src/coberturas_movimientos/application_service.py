from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.coberturas_movimientos.dto import CobeberturasMovimientosDateRange

class LoadCoberturasMovimientos:
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self,dto: CobeberturasMovimientosDateRange):
    
    init_date = dto.init_date
    final_date = dto.final_date
    
    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_COBERTURAS_MOVIMIENTOS` WHERE CAST(FECHA_MOVIMIENTO AS DATE) BETWEEN '{init_date}' AND '{final_date}';", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_COBERTURAS_MOVIMIENTOS",
      schema="INSUMOS",
      table="DM_COBERTURAS_MOVIMIENTOS"
    )    
    
    return response