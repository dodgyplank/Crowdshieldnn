import pandas as pd
from shapely import wkt

df = pd.read_csv("data/processed/master_dataset.csv")
# Pick the columns with the most numeric data instead of the first match
lat_cols = ['_lat', 'lat', 'latitude', 'y']
lon_cols = ['_lon', 'lon', 'longitude', 'x']

def choose_best_column(df, candidates):
    best_col = None
    max_count = -1
    for c in candidates:
        if c in df.columns:
            numeric_count = pd.to_numeric(df[c], errors='coerce').notna().sum()
            if numeric_count > max_count:
                max_count = numeric_count
                best_col = c
    return best_col

lat_col = choose_best_column(df, lat_cols)
lon_col = choose_best_column(df, lon_cols)

print(f"Using latitude: {lat_col}, longitude: {lon_col}")

# Convert to numeric
df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')

# Drop rows without coordinates
df = df.dropna(subset=[lat_col, lon_col])
print(f"Number of geolocated rows: {len(df)}")
