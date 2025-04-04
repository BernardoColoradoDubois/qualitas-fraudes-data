import requests
from datetime import datetime, timedelta

def date_interval_generator(init_date, final_date, **kwargs):

  date_range= {
    'init-date': init_date,
    'final-date': final_date
  }
  
  print(date_range)
  return date_range

def load_api_data_by_date_range(url,date_generator_task_id,api_key,**kwargs):

  date_range = kwargs['ti'].xcom_pull(task_ids=date_generator_task_id)

  headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
  }
  response = requests.post(url, headers=headers, json=date_range)  
  print(response.json())


def load_api_data(url,api_key,**kwargs):

  headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
  }
  response = requests.post(url, headers=headers)  
  print(response.json())
