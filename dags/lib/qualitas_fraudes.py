import requests
from datetime import datetime, timedelta

def date_interval_generator(init_date, final_date, **kwarg):

  date_range= {
    'init-date': init_date,
    'final-date': final_date
  }
  
  print(date_range)
  return date_range

def load_api_data_by_date_range(url,date_generator_task_id,api_key,**context):

  date_range = context['ti'].xcom_pull(task_ids=date_generator_task_id)
  print(f"Date range from {date_range}")
    
  #response = requests.post(url, headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})

