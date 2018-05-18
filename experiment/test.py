import numpy as np
import pandas as pd
df = pd.read_csv('daily_changes.csv', sep=',')
df = df.drop(['date'], 1)
df = df.assign(max=df.max(axis=1))

columns = df.columns[df.columns != 'max']
for column in columns:
    df.loc[df[column] != df['max'], column] = 0
for column in columns:
    df.loc[df[column] == df['max'], column] = 1

df = df.drop(['max'], 1)
df = df.applymap(lambda x: int(x))
df.apply(lambda x: x.value_counts(), axis=1)
print(df)
