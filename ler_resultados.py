import pandas as pd

# print("--- TABELA 1: MÉDIA DE RISK SCORE ---")
# df_tabela1 = pd.read_parquet("data/output/tabela1_risk_score")
# print(df_tabela1.head())

# print("\n--- TABELA 2: TOP 3 SALES ---")
# df_tabela2 = pd.read_parquet("data/output/tabela2_top3_sales")
# print(df_tabela2.head())

df = pd.read_csv("data/input/df_fraud_credit.csv", sep=",",na_values="none")
# 1. Filtrar somente transaction_type == 'sale'
df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
df_sales = df[df['transaction_type'] == 'sale'].copy()

# 2. Ordenar por timestamp (descendente) para garantir que a transação mais recente fique no topo
df_sales = df_sales.sort_values(by='timestamp', ascending=False)

# 3. Remover duplicatas de receiving_address, mantendo a primeira (a mais recente após o sort)
df_latest_sales = df_sales.drop_duplicates(subset='receiving_address', keep='first')

# 4. Listar os 3 com maior 'amount'
top_3_sales = df_latest_sales.nlargest(3, 'amount')[['receiving_address', 'amount', 'timestamp']]

print(top_3_sales)


# 1. Tratamento inicial (considerando que 'none' pode aparecer também no risk_score)
df['risk_score'] = pd.to_numeric(df['risk_score'], errors='coerce').fillna(0)

# 2. Agrupar por região e calcular a média do risk_score
ranking_regioes = df.groupby('location_region')['risk_score'].mean()

# 3. Ordenar de forma decrescente
ranking_regioes = ranking_regioes.sort_values(ascending=False)

print(ranking_regioes)

df_s = df[df['location_region'] == '0'].copy()
print(df_s.head())