from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.registro.dto import RegistroDateRange

class LoadRegistro:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: RegistroDateRange):
    
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_REGISTRO` WHERE CAST(FECHA_ASIGNACION AS DATE) BETWEEN '{init_date}' AND '{final_date}';", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_REGISTRO",
      schema="INSUMOS",
      table="DM_REGISTRO"
    )    
    
    return response