from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadSiniestros:
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    self.bigquery_to_oracle = bigquery_to_oracle

  def invoque(self):
    ...
