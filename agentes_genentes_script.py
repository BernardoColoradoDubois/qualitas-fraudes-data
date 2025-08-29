import pandas as pd

sheet_name = 'Hoja1'

file = './files/Agentes_Gerentes.xlsx'

column_names = [
  'CODIGO_OFICINA_OFICINA',
  'CODIGO_GERENTE_GERENTE',
  'CODIGO_AGENTE',
  'AGENTE',
]

dtypes = {
  'CODIGO_OFICINA_OFICINA': 'string',
  'CODIGO_GERENTE_GERENTE': 'string',
  'CODIGO_AGENTE': 'string',
  'AGENTE': 'string',
}

df = pd.read_excel(file,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

df[['CODIGO_OFICINA', 'OFICINA']] = df['CODIGO_OFICINA_OFICINA'].str.split(' ', n=1, expand=True)
df[['CODIGO_GERENTE', 'GERENTE']] = df['CODIGO_GERENTE_GERENTE'].str.split(' ', n=1, expand=True)

df = df.drop(columns=['CODIGO_OFICINA_OFICINA'])
df = df.drop(columns=['CODIGO_GERENTE_GERENTE'])

gerentes_df = df[['CODIGO_OFICINA' ,'CODIGO_GERENTE' ,'GERENTE']].copy().drop_duplicates(subset=['CODIGO_GERENTE'])

agentes_df = df[['CODIGO_OFICINA' ,'CODIGO_GERENTE' ,'CODIGO_AGENTE' ,'AGENTE']].copy().drop_duplicates(subset=['CODIGO_AGENTE'])


print(gerentes_df.head())
print(agentes_df.head())


gerentes_df.to_csv('./csv/GERENTES.csv', index=False, encoding='utf-8-sig')
agentes_df.to_csv('./csv/AGENTES.csv', index=False, encoding='utf-8-sig')