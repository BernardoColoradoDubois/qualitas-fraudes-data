from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadCoberturasMovimientos:
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    self.bigquery_to_oracle = BigQueryToOracle()

  def invoque(self):
    ...
