#!/usr/bin/env python3
"""
Process datasets in ./data (raw and processed) and consolidate into a master tabular dataset.
Saves outputs to data/processed/master_dataset.parquet and master_dataset.csv

Usage:
    python scripts/process_all.py --data-dir data --out-dir data/processed

This script tries to be robust: it will parse GeoJSON FeatureCollections, arrays of JSON objects,
CSV files and simple XML files (via xmltodict). Geometry centroids are computed with shapely
when geometry is present.
"""
import os
import json
import argparse
from glob import glob
from pathlib import Path
from typing import List

import pandas as pd
from pandas import json_normalize

try:
    from shapely.geometry import shape, Point
except Exception:
    shape = None

try:
    import xmltodict
except Exception:
    xmltodict = None


def find_files(data_dir: str, exts: List[str] = None) -> List[str]:
    if exts is None:
        exts = ['.json', '.geojson', '.csv', '.xml']
    out = []
    for root, dirs, files in os.walk(data_dir):
        for f in files:
            if any(f.lower().endswith(ext) for ext in exts):
                out.append(os.path.join(root, f))
    return out


def try_parse_geojson(fp: str):
    with open(fp, 'r', encoding='utf-8') as f:
        data = json.load(f)
    features = []
    if isinstance(data, dict) and 'features' in data:
        for feat in data['features']:
            prop = feat.get('properties', {}) or {}
            geom = feat.get('geometry')
            geom_type = None
            lon = lat = None
            if geom is not None and shape is not None:
                try:
                    geom_obj = shape(geom)
                    geom_type = geom.get('type')
                    centroid = geom_obj.centroid
                    lon = centroid.x
                    lat = centroid.y
                except Exception:
                    geom_obj = None
            # Flatten properties
            row = json_normalize(prop, sep='_').to_dict(orient='records')
            row = row[0] if row else {}
            row.update({
                '_source_file': fp,
                '_geom_type': geom_type,
                '_lon': lon,
                '_lat': lat,
            })
            features.append(row)
    return features


def try_parse_json_array(fp: str):
    with open(fp, 'r', encoding='utf-8') as f:
        data = json.load(f)
    rows = []
    if isinstance(data, list):
        for obj in data:
            if isinstance(obj, dict):
                row = json_normalize(obj, sep='_').to_dict(orient='records')
                row = row[0] if row else {}
                row.update({'_source_file': fp})
                rows.append(row)
    elif isinstance(data, dict):
        # Some JSON files are top-level dicts with useful keys
        # Try to flatten if one key maps to a list of objects
        for k, v in data.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                for obj in v:
                    row = json_normalize(obj, sep='_').to_dict(orient='records')
                    row = row[0] if row else {}
                    row.update({'_source_file': fp, '_root_key': k})
                    rows.append(row)
                break
    return rows


def try_parse_csv(fp: str):
    try:
        df = pd.read_csv(fp)
    except Exception:
        try:
            df = pd.read_csv(fp, engine='python')
        except Exception:
            return []
    if df.shape[0] == 0:
        return []
    df['_source_file'] = fp
    # Attempt to standardize lat/lon columns if present
    for col in ['lat', 'latitude', 'y', 'lat_deg']:
        if col in df.columns:
            df.rename(columns={col: '_lat'}, inplace=True)
            break
    for col in ['lon', 'longitude', 'x', 'lon_deg']:
        if col in df.columns:
            df.rename(columns={col: '_lon'}, inplace=True)
            break
    return df.to_dict(orient='records')


def try_parse_xml(fp: str):
    if xmltodict is None:
        return []
    with open(fp, 'r', encoding='utf-8') as f:
        txt = f.read()
    try:
        d = xmltodict.parse(txt)
    except Exception:
        return []
    # Find first list of dicts
    rows = []
    def walk(obj):
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            return obj
        if isinstance(obj, dict):
            for v in obj.values():
                r = walk(v)
                if r:
                    return r
        return None
    candidate = walk(d)
    if candidate:
        for item in candidate:
            row = json_normalize(item, sep='_').to_dict(orient='records')
            row = row[0] if row else {}
            row.update({'_source_file': fp})
            rows.append(row)
    return rows


def process_file(fp: str):
    ext = fp.lower().split('.')[-1]
    rows = []
    try:
        if ext in ['json', 'geojson']:
            rows = try_parse_geojson(fp)
            if not rows:
                rows = try_parse_json_array(fp)
        elif ext == 'csv':
            rows = try_parse_csv(fp)
        elif ext == 'xml':
            rows = try_parse_xml(fp)
    except Exception as e:
        print(f"Error parsing {fp}: {e}")
    return rows


def main(data_dir: str, out_dir: str, limit_files: int = None):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    files = find_files(data_dir)
    print(f"Found {len(files)} files to inspect under {data_dir}")
    if limit_files:
        files = files[:limit_files]
    all_rows = []
    for i, fp in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Processing: {fp}")
        rows = process_file(fp)
        if rows:
            all_rows.extend(rows)
    if not all_rows:
        print("No tabular rows extracted. Exiting.")
        return 1
    print(f"Collected {len(all_rows)} rows. Building DataFrame...")
    df = pd.DataFrame(all_rows)
    # Normalize column names
    df.columns = [c if isinstance(c, str) else str(c) for c in df.columns]
    # Reorder so source file and geometry columns come first if present
    cols = list(df.columns)
    front = [c for c in ['_source_file', '_root_key', '_geom_type', '_lon', '_lat'] if c in cols]
    rest = [c for c in cols if c not in front]
    df = df[front + rest]
    # Save
    out_csv = os.path.join(out_dir, 'master_dataset.csv')
    out_parquet = os.path.join(out_dir, 'master_dataset.parquet')
    df.to_csv(out_csv, index=False)
    try:
        df.to_parquet(out_parquet, index=False)
    except Exception as e:
        print(f"Warning: could not save parquet ({e}). You may need pyarrow or fastparquet installed.")
    print(f"Saved master CSV → {out_csv}")
    print(f"Saved master Parquet → {out_parquet} (if supported)")
    return 0


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--data-dir', default='data', help='Root data directory to scan')
    p.add_argument('--out-dir', default=os.path.join('data', 'processed'), help='Output directory for master dataset')
    p.add_argument('--limit', type=int, default=0, help='Optional: limit to first N files for testing')
    args = p.parse_args()
    limit = args.limit if args.limit > 0 else None
    exit(main(args.data_dir, args.out_dir, limit))
