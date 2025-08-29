import pandas as pd

sheet_name = 'CAT_OFICINAS'

file = './sas/CIENCIA_DATOS_CATALOGO_DIRECCION_COMERCIAL_Catalogo_direccion_comercial.xlsx'

column_names = [
  'NO_OF'
  ,'OFICINA'
  ,'ZONA_ATENCION'
  ,'DIRECTOR_GENERAL'
  ,'SUBDIRECTOR_GENERAL'
]

dtypes = {
  'NO_OF': 'string',
  'OFICINA': 'string',
  'ZONA_ATENCION': 'string',
  'DIRECTOR_GENERAL': 'string',
  'SUBDIRECTOR_GENERAL': 'string',
}

df = pd.read_excel(file,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

df.to_csv('./csv/CATALOGO_DIRECCION_COMERCIAL.csv', index=False, encoding='utf-8-sig')