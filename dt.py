import pandas as pd

df = pd.read_csv('data.csv')

# calculate diff t of "timestamp" column
df['dt'] = df["timestamp"].diff()

# save back to data.csv
df.to_csv('data.csv', index=False)

# sort by dt, print out largest dts and indices
df = df.sort_values(by='dt', ascending=False)

print(df.head(5))

# print out general stats of the dt column, 99th and 99.9th percentiles
print(df['dt'].quantile([0.99, 0.999, 0.9999]))

# mean and std of dt
print(f"mean: {df['dt'].mean()}\nstd: {df['dt'].std()}")