import requests
from datetime import datetime, timedelta

def date_interval_generator(init_date, final_date, **kwarg):

  date_range= {
    'init-date': init_date,
    'final-date': final_date
  }
  
  print(date_range)
  return date_range

def load_api_data_by_date_range(url,api_key,origin_task_id, **kwargs):

  ti = kwargs['ti']
  
  print(f"ti: {ti}")
  
  #response = requests.post(url, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})

  
def load_api_data(url,api_key,**kwargs):
  pass
