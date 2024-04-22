import pandas as pd

df = pd.read_csv('data.csv')

# calculate diff t of "timestamp" column
df['dt'] = df["timestamp"].diff()

# save
df.to_csv('data.csv', index=False)