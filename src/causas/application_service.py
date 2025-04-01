from src.lib.bigquery_to_oracle import BigQueryToOracle

class LoadCausas:
  def __init__(self,bigquery_to_oracle: BigQueryToOracle):
    self.bigquery_to_oracle = BigQueryToOracle()

  def invoque(self):
    ...
