# walkthru-pop-index

Global population projections ([WorldPop SSP 1km](https://www.worldpop.org/)) aggregated to [H3](https://h3geo.org/) hexagonal cells and published as [GeoParquet](https://geoparquet.org/).

Part of the [walkthru-earth](https://github.com/walkthru-earth) index family.

## Output

Hosted on [Source Cooperative](https://source.coop/):

```
s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/
  scenario=SSP2/
    h3_res=1/data.parquet     # 430 cells
    h3_res=2/data.parquet     # 2,222 cells
    ...
    h3_res=8/data.parquet     # 44,888,216 cells
    _metadata.json
```

**Schema** — each Parquet file contains:

| Column | Type | Description |
|---|---|---|
| `h3_index` | string | H3 cell ID |
| `geometry` | GEOMETRY | Cell center (POINT, EPSG:4326) |
| `lat`, `lon` | float32 | Cell center coordinates |
| `area_km2` | float32 | Cell area in km² |
| `pop_2025` … `pop_2100` | float32 | Projected population count (5-year steps) |

## Quick start

```bash
uv sync
uv run python main.py --dry-run                    # preview
uv run python main.py --scenario SSP2 --workers 32  # run
```

**CLI options:**

| Flag | Default | Description |
|---|---|---|
| `--scenario` | `SSP2` | SSP scenario (SSP1–SSP5) |
| `--resolutions` | `1,2,3,4,5,6,7,8` | H3 resolutions |
| `--workers` | `nproc - 2` | Parallel worker count |
| `--scratch-dir` | `/data/scratch/pop` | Working directory |
| `--skip-download` | — | Reuse existing rasters |
| `--dry-run` | — | Preview without processing |

## Query the output

```sql
-- DuckDB
SELECT h3_index, pop_2025, pop_2050, pop_2100,
       (pop_2100 / pop_2025) AS growth_ratio
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/scenario=SSP2/h3_res=5/data.parquet')
ORDER BY pop_2025 DESC
LIMIT 10;
```

## Data source

**WorldPop Global SSP Projections v0.2** — 30 arc-second (~1 km) gridded population projections for 2025–2100 under five Shared Socioeconomic Pathways.

- DOI: [10.5258/SOTON/WP00849](https://doi.org/10.5258/SOTON/WP00849)
- CRS: EPSG:4326 (WGS 84)
- Aggregation: SUM per H3 cell

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links)
