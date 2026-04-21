import pandas as pd

# params
INPUT_PATH = "scripts/data/sample_credit_usage.csv"
OUTPUT_PATH = "scripts/data/churn_candidates.csv"
THRESHOLD = 0.3

# leitura
df = pd.read_csv(INPUT_PATH)

# validação
if "usage_rate" not in df.columns:
    raise ValueError("Coluna 'usage_rate' não encontrada")

# filtro
churn_df = df[df["usage_rate"] < THRESHOLD]

# saída
churn_df.to_csv(OUTPUT_PATH, index=False)

print(f"{len(churn_df)} empresas com baixa utilização salvas em {OUTPUT_PATH}")