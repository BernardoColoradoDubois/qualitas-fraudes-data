

class BigQueryToOracle:
  def __init__(self, bq_client, oracle_client):
    self.bq_client = bq_client
    self.oracle_client = oracle_client
    
  def run(self, extraction_query,preload_query,schema,table):
    
    query_job = self.bq_client.query(extraction_query)
    result = query_job.result()
    df = result.to_dataframe()    
    dt = [tuple(x) for x in df.values]
    
    cursor = self.oracle_client.cursor()
    
    if preload_query is not None:
      cursor.execute(preload_query)
    
    insert_query = f"INSERT INTO {schema}.{table}"+" VALUES("+",".join([f":{i+1}" for i in range(df.shape[1])])+")"
    
    cursor.executemany(insert_query, dt)
    self.oracle_client.commit()
    cursor.close()



