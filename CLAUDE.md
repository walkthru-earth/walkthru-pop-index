# CLAUDE.md

## Project

**walkthru-pop-index** — Global population projections (WorldPop SSP 1km) → H3-indexed Parquet with native Parquet 2.11+ GEOMETRY.

Part of the [walkthru-earth](https://github.com/walkthru-earth) index family alongside `dem-terrain`, `walkthru-building-index`, and `walkthru-weather-index`.

## Commands

```bash
# Setup
uv sync

# Lint & format (ALWAYS before committing)
uv run ruff check . --fix
uv run ruff format .

# Inspect data
uv run python inspection/inspect_raster.py /path/to/worldpop/SSP2/
uv run python inspection/query_output.py

# Run pipeline (use tmux for long-running jobs)
uv run python main.py --scenario SSP2 --resolutions 1,2,3,4,5,6,7,8 --workers 178
uv run python main.py --dry-run                    # Preview windows
uv run python main.py --skip-download              # Reuse existing data
```

## Source data

- **WorldPop Global SSP Projections v0.2**: `https://data.worldpop.org/repo/prj/FuturePop/SSPs_1km_v0_2`
- 30 arc-second (~1 km) gridded population projections, 2025–2100, five SSP scenarios
- Citation: WorldPop (2018). Global 1km-grid population projections, v0.2. University of Southampton. [doi:10.5258/SOTON/WP00849](https://doi.org/10.5258/SOTON/WP00849)

## Architecture

```
WorldPop S3 (public ZIP) → Download & Extract → GeoTIFFs on NVMe
        ↓
Phase 1: discover_rasters() — detect year from filename
Phase 2: generate_windows() — 5° × 5° geographic tiles
Phase 3: ProcessPoolExecutor(N workers)
          ├─ Read raster window (rasterio)
          ├─ H3 cell assignment per pixel (h3.latlng_to_cell)
          ├─ GroupBy H3 cell → SUM population
          └─ Write temp Parquet (pyarrow)
Phase 4: DuckDB merge per resolution
          ├─ GROUP BY h3_index across windows (boundary dedup)
          ├─ Add ST_Point geometry (native Parquet 2.11+)
          └─ Write final sorted Parquet with GEOPARQUET_VERSION 'BOTH'
Phase 5: _metadata.json
```

## S3 output layout

```
s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/
  scenario=SSP2/
    h3_res=1/data.parquet
    h3_res=2/data.parquet
    ...
    h3_res=8/data.parquet
    _metadata.json
```

Each Parquet file: `h3_index, geometry, lat, lon, area_km2, pop_2025, pop_2030, ..., pop_2100`

## Key design decisions

- **SUM aggregation**: population counts are summed when multiple raster pixels map to one H3 cell (correct for count data, unlike mean for elevation)
- **multiprocessing.ProcessPoolExecutor**: true CPU parallelism (bypasses GIL), uses `spawn` context for rasterio safety
- **Window-boundary dedup**: same H3 cell can appear in adjacent windows; final DuckDB GROUP BY SUM resolves duplicates
- **Native Parquet GEOMETRY**: DuckDB 1.5.0 spatial writes `ST_Point(lon, lat)::GEOMETRY('EPSG:4326')` with `GEOPARQUET_VERSION 'BOTH'` — native Parquet 2.11+ GEOMETRY logical type (per-row-group bbox stats) AND GeoParquet 1.0 file-level metadata for backwards compatibility
- **Checkpoint/resume**: completed windows tracked in checkpoint.json; safe to kill and restart

## Code conventions

- **Always use `pathlib.Path`** instead of `os.path` for all path operations
- **Progressive S3 upload**: upload each resolution's Parquet immediately after merge (don't wait for all resolutions to finish)
- **`uv add`** for dependency management — never edit pyproject.toml manually
- **`uv run ruff format . && uv run ruff check . --fix`** before every commit

## Documentation files

- `README.md` — GitHub repo README (code usage)
- `SC_README.md` — Source Cooperative dataset README (uploaded to S3 as `indices/population/README.md`)

## File layout

```
main.py              Pipeline entrypoint
inspection/
  inspect_raster.py  gdalinfo + pixel sampling on WorldPop GeoTIFFs
  query_output.py    DuckDB queries on output Parquet files
  upload_s3.py       Upload output to S3
  download_sample.py Download and peek at WorldPop ZIP contents
  test_duckdb_geo.py Verify DuckDB native Parquet GEOMETRY output
pyproject.toml       Dependencies (rasterio, h3, duckdb==1.5.0.dev329, numpy, pyarrow)
CLAUDE.md            This file
```

## License

CC BY 4.0 by walkthru-earth. Source data by University of Southampton (WorldPop), CC BY 4.0.
