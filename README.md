# walkthru-pop-index

Global population projections ([WorldPop SSP 1km](https://www.worldpop.org/)) aggregated to [H3](https://h3geo.org/) hexagonal cells and published as [GeoParquet](https://geoparquet.org/).

Part of the [walkthru-earth](https://github.com/walkthru-earth) index family.

## Output

Hosted on [Source Cooperative](https://source.coop/):

```
s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/
  v1/scenario=SSP2/                          # legacy (VARCHAR h3_index, has geometry/lat/lon/area_km2)
    h3_res=1/data.parquet     # 430 cells
    ...
    h3_res=8/data.parquet     # 44,888,216 cells
  v2/scenario=SSP2/                          # recommended (BIGINT h3_index, optimized)
    h3_res=1/data.parquet     # 430 cells
    h3_res=2/data.parquet     # 2,222 cells
    ...
    h3_res=8/data.parquet     # 44,888,216 cells
    _metadata.json
```

**Schema (v2, recommended)** — each Parquet file contains:

| Column | Type | Description |
|---|---|---|
| `h3_index` | BIGINT | H3 cell ID (integer representation) |
| `pop_2025` … `pop_2100` | float32 | Projected population count (5-year steps) |

17 columns total. Geometry, lat/lon, and area_km2 are derivable from `h3_index` via the DuckDB `h3` extension.

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
-- DuckDB (v2)
INSTALL h3 FROM community; LOAD h3;
INSTALL httpfs;             LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       pop_2025, pop_2050, pop_2100,
       (pop_2100 / pop_2025) AS growth_ratio
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet')
ORDER BY pop_2025 DESC
LIMIT 10;
```

## Source

**WorldPop Global SSP Projections v0.2** — 30 arc-second (~1 km) gridded population projections for 2025–2100 under five Shared Socioeconomic Pathways.

> WorldPop. (2018). Global 1km-grid population projections, v0.2. University of Southampton. [doi:10.5258/SOTON/WP00849](https://doi.org/10.5258/SOTON/WP00849)

## License

This project is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://github.com/walkthru-earth). See [LICENSE](LICENSE) for details. The source [WorldPop SSP Projections](https://doi.org/10.5258/SOTON/WP00849) is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by the [University of Southampton](https://www.worldpop.org/).

Contact: [hi@walkthru.earth](mailto:hi@walkthru.earth)
