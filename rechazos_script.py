import pandas as pd

sheet_name = 'Hoja1'

file = './files/RECHAZOS.xlsx'

column_names = [
  'SINIESTRO',
  'CAUSA',
  'DETALLE',
  'POLIZA',
  'INCISO',
  'ASEGURADO',
  'SERIE',
  'ESTATUS',
  'AHORRO',
  'MES_RECHAZO',
  'CVE_AGENTE',
  'AGENTE',
]

dtypes = {
  'SINIESTRO': 'string',
  'CAUSA': 'string',
  'DETALLE': 'string',
  'POLIZA': 'string',
  'INCISO': 'string',
  'ASEGURADO': 'string',
  'SERIE': 'string',
  'ESTATUS': 'string',
  'AHORRO': 'string',
  'MES_RECHAZO': 'string',
  'CVE_AGENTE': 'string',
  'AGENTE': 'string',
}

df = pd.read_excel(file,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

df.to_csv('./csv/RECHAZOS.csv', index=False, encoding='utf-8-sig')