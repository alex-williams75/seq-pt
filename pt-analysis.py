import pandas as pd

df = pd.read_parquet('SEQ_PT_Trips.parquet')

print(df.describe())