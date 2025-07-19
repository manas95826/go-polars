import go_polars as gp

# Create a DataFrame
data = {
    'A': [1, 2, 3, 4, 5],
    'B': [10.0, 20.0, 30.0, 40.0, 50.0],
    'C': [True, False, True, False, True]
}
df = gp.DataFrame.from_dict(data)

# Basic operations
print(df.head())
print(df.describe())

# Filtering
filtered = df[df['A'] > 2]

# Sorting
sorted_df = df.sort_values('B', ascending=False)

# GroupBy operations
grouped = df.groupby('C').agg({'A': 'sum', 'B': 'mean'})