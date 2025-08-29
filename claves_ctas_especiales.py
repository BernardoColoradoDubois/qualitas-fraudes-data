import pandas as pd

sheet_name = 'Hoja1'

file = './sas/CLAVES_CTAS_ESPECIALES 3.xlsx'

column_names = [
  'CUENTA'
  ,'CVE_AGENTE'
  ,'EJECUTIVA'
]

dtypes = {
  'CUENTA': 'string',
  'CVE_AGENTE': 'string',
  'EJECUTIVA': 'string',
}

df = pd.read_excel(file,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

df.to_csv('./csv/CLAVES_CTAS_ESPECIALES.csv', index=False, encoding='utf-8-sig')