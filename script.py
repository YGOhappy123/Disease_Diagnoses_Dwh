import pandas as pd
import json

df = pd.read_csv("disease_diagnosis.csv")

df2 = df[["disease"]].copy()
df2["symptoms"] = df.drop(columns=["disease"]).apply(
    lambda row: json.dumps([symptom.strip() for symptom in row.dropna().astype(str)]), axis=1
)

# Save as a UTF-8 text file
df2.to_csv(
    "datasets/source_kaggle/disease_diagnosis.txt",
    index=False,
    encoding="utf-8",
    sep='\t',
    quotechar="'",
)

print(df2.iloc[0]['symptoms'])
