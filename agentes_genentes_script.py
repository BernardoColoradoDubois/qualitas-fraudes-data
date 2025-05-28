import pandas as pd

sheet_name = 'Hoja1'

file = './files/Agentes_Gerentes.xlsx'

column_names = [
  'codigo_oficina_oficina',
  'codigo_gerente_gerente',
  'codigo_agente',
  'agente',
]

dtypes = {
  'codigo_oficina_oficina': 'string',
  'codigo_gerente_gerente': 'string',
  'codigo_agente': 'string',
  'agente': 'string',
}

df = pd.read_excel(file,engine='openpyxl',sheet_name=sheet_name,skiprows=1,header=None,names=column_names,index_col=False,dtype=dtypes)

df[['codigo_oficina', 'oficina']] = df['codigo_oficina_oficina'].str.split(' ', n=1, expand=True)
df[['codigo_gerente', 'gerente']] = df['codigo_gerente_gerente'].str.split(' ', n=1, expand=True)

df = df.drop(columns=['codigo_oficina_oficina'])
df = df.drop(columns=['codigo_gerente_gerente'])

gerentes_df = df[['codigo_oficina' ,'codigo_gerente' ,'gerente']].copy().drop_duplicates(subset=['codigo_gerente'])

agentes_df = df[['codigo_oficina' ,'codigo_gerente' ,'codigo_agente' ,'agente']].copy().drop_duplicates(subset=['codigo_agente'])


print(gerentes_df.head())
print(agentes_df.head())


gerentes_df.to_csv('./csv/gerentes.csv', index=False, encoding='utf-8-sig')
agentes_df.to_csv('./csv/agentes.csv', index=False, encoding='utf-8-sig')