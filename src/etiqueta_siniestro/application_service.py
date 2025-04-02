from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.etiqueta_siniestro.dto import EtiquetaSiniestroDateRange

class LoadEtiquetaSiniestro:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: EtiquetaSiniestroDateRange):
  
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_ETIQUETA_SINIESTRO` WHERE CAST(FECHA_CARGA AS DATE) BETWEEN '{init_date}' AND '{final_date}';", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_ETIQUETA_SINIESTRO",
      schema="INSUMOS",
      table="DM_ETIQUETA_SINIESTRO"
    ) 
      
    return response