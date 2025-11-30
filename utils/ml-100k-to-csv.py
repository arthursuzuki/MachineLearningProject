import pandas as pd

df = pd.read_csv("utils/ml-100k/u.data", sep="\t", names=["user_id", "item_id", "rating", "timestamp"])
df.to_csv("utils/ratings.csv", index=False)

print(df.head())
