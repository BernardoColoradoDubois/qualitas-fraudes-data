from src.lib.bigquery_to_oracle import BigQueryToOracle
from src.pagos_proveedores.dto import PagosProveedoresDateRange

class LoadPagosProveedores:
  
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self, dto: PagosProveedoresDateRange):
    
    init_date = dto.init_date
    final_date = dto.final_date

    response = self.bigquery_to_oracle.run(
      extraction_query=f"SELECT * FROM `qualitasfraude.DM_FRAUDES.DM_PAGOS_PROVEEDORES`WHERE CAST(FECHA_PAGO AS DATE) BETWEEN '{init_date}' AND '{final_date}';", 
      preload_query="TRUNCATE TABLE INSUMOS.DM_PAGOS_PROVEEDORES",
      schema="INSUMOS",
      table="DM_PAGOS_PROVEEDORES"
    )    
    
    return response