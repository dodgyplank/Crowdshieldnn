Dataset processing
===================

Run the consolidation script to parse all data files under `data/` and build a single master dataset.

Install required packages (preferably in a virtualenv):

```bash
pip install -r requirements.txt
```

Run the script:

```bash
python scripts/process_all.py --data-dir data --out-dir data/processed
```

Outputs:
- `data/processed/master_dataset.csv`
- `data/processed/master_dataset.parquet` (if pyarrow or fastparquet available)

Notes:
- The script attempts to parse GeoJSON FeatureCollections (extracting properties and computing centroids), JSON arrays of objects, CSV files and simple XML files.
- It will add a `_source_file` column with the original file path for traceability.
- For GeoJSON geometries, `_geom_type`, `_lon`, `_lat` columns are created (centroid for non-point geometries).
- The script is conservative: if a library (shapely, xmltodict) is not installed, those parsing paths will be skipped or limited.

If you'd like additional normalization (e.g., unified timestamps, column renames, coordinate reprojection), tell me which fields to target and I can extend the script.
