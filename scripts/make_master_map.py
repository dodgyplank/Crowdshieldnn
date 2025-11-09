#!/usr/bin/env python3
"""
Create a Folium map from the consolidated master dataset.
Saves HTML to data/processed/master_map.html

Usage:
    python scripts/make_master_map.py --input data/processed/master_dataset.csv --out data/processed/master_map.html
"""
import argparse
import os
import pandas as pd
import folium
from folium import plugins

def choose_best_column(df, candidates):
    """Return the column with the most numeric values among candidates."""
    best_col = None
    max_count = -1
    for c in candidates:
        if c in df.columns:
            numeric_count = pd.to_numeric(df[c], errors='coerce').notna().sum()
            if numeric_count > max_count:
                max_count = numeric_count
                best_col = c
    return best_col

def extract_numeric_series(s: pd.Series) -> pd.Series:
    """Convert series to numeric, fallback to extracting numbers from strings."""
    out = pd.to_numeric(s, errors='coerce')
    if out.notna().sum() > 0:
        return out
    extracted = s.astype(str).str.extract(r'([-+]?\d*\.\d+|[-+]?\d+)')
    if extracted.shape[1] >= 1:
        return pd.to_numeric(extracted[0], errors='coerce')
    return out

def main(input_fp: str, out_fp: str, min_count: int = 1):
    if not os.path.exists(input_fp):
        raise SystemExit(f"Input file not found: {input_fp}")

    df = pd.read_csv(input_fp, low_memory=False)

    # Pick the latitude/longitude columns with the most numeric values
    lat_col = choose_best_column(df, ['_lat', 'lat', 'latitude', 'y'])
    lon_col = choose_best_column(df, ['_lon', 'lon', 'longitude', 'x'])
    print(f"Using latitude: {lat_col}, longitude: {lon_col}")

    # If not found, try geometry columns
    geom_candidate = None
    if (lat_col is None or lon_col is None):
        for key in ['geometry', 'geom', 'coordinates']:
            if key in df.columns:
                geom_candidate = key
                break
        if geom_candidate:
            import json as _json
            lats, lons = [], []
            for val in df[geom_candidate].fillna(''):
                try:
                    obj = _json.loads(val)
                    if isinstance(obj, dict) and 'coordinates' in obj:
                        coords = obj['coordinates']
                        lons.append(coords[0])
                        lats.append(coords[1])
                        continue
                    if isinstance(obj, list) and len(obj) >= 2:
                        lons.append(obj[0])
                        lats.append(obj[1])
                        continue
                except Exception:
                    continue
            if lats and lons:
                df['_lat_extracted'] = pd.to_numeric(lats, errors='coerce')
                df['_lon_extracted'] = pd.to_numeric(lons, errors='coerce')
                lat_col = '_lat_extracted'
                lon_col = '_lon_extracted'

    # Convert to numeric
    if lat_col and lon_col:
        df[lat_col] = extract_numeric_series(df[lat_col])
        df[lon_col] = extract_numeric_series(df[lon_col])
    else:
        raise SystemExit('No latitude/longitude columns found.')

    # Drop rows without coordinates
    df = df.dropna(subset=[lat_col, lon_col])
    if len(df) < min_count:
        raise SystemExit(f'Not enough geolocated rows ({len(df)}) to build a map.')

    print(f"Number of geolocated rows: {len(df)}")

    # Compute map center
    center_lat = float(df[lat_col].mean())
    center_lon = float(df[lon_col].mean())

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    # Heatmap layer
    heat_points = df[[lat_col, lon_col]].values.tolist()
    if heat_points:
        plugins.HeatMap(heat_points, radius=8, blur=10, min_opacity=0.2).add_to(m)

    # Clustered markers
    marker_cluster = plugins.MarkerCluster(name='Points').add_to(m)
    popup_cols = [c for c in ['_source_file', 'NAME', 'name', 'id', 'road_name', 'address', 'Description'] if c in df.columns]

    for _, row in df.iterrows():
        lat = float(row[lat_col])
        lon = float(row[lon_col])
        details = [f"<b>{col}</b>: {row[col]}" for col in popup_cols if pd.notna(row[col])]
        popup = folium.Popup('<br/>'.join(details), max_width=300) if details else None
        folium.CircleMarker(location=[lat, lon], radius=4, color='blue', fill=True, popup=popup).add_to(marker_cluster)

    folium.LayerControl().add_to(m)
    os.makedirs(os.path.dirname(out_fp), exist_ok=True)
    m.save(out_fp)
    print(f"Saved map to {out_fp} (points: {len(df)})")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', default=os.path.join('data', 'processed', 'master_dataset.csv'))
    parser.add_argument('--out', '-o', default=os.path.join('data', 'processed', 'master_map.html'))
    args = parser.parse_args()
    main(args.input, args.out)
