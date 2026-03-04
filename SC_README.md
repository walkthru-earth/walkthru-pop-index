# Global Population Projections (H3-indexed)

WorldPop SSP 1km population projections (2025–2100) aggregated to [H3](https://h3geo.org/) hexagonal cells in [native Parquet 2.11+ GEOMETRY](https://github.com/apache/parquet-format/blob/master/Geospatial.md) format. Eight H3 resolutions (1–8), one file per resolution, sorted by `h3_index`.

| | |
|---|---|
| **Source** | [WorldPop](https://www.worldpop.org/) Global SSP Projections v0.2, 30 arc-second (~1 km), global coverage |
| **Format** | Apache Parquet with native GEOMETRY logical type (DuckDB 1.5) |
| **CRS** | EPSG:4326 (WGS 84) |
| **License** | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links) |
| **Code** | [walkthru-earth/walkthru-pop-index](https://github.com/walkthru-earth/walkthru-pop-index) |

## Quick Start

```sql
-- DuckDB
INSTALL spatial; LOAD spatial;
INSTALL httpfs;  LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index, pop_2025, pop_2050, pop_2100,
       (pop_2100 / pop_2025)::FLOAT AS growth_ratio
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/scenario=SSP2/h3_res=5/data.parquet')
ORDER BY pop_2025 DESC
LIMIT 20;
```

```python
# Python
import duckdb

con = duckdb.connect()
for ext in ("spatial", "httpfs"):
    con.install_extension(ext); con.load_extension(ext)
con.sql("SET s3_region = 'us-west-2'")

df = con.sql("""
    SELECT h3_index, lat, lon, pop_2025, pop_2050, pop_2100
    FROM read_parquet(
        's3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/scenario=SSP2/h3_res=5/data.parquet'
    ) WHERE lat BETWEEN 20 AND 35 AND lon BETWEEN 68 AND 90
""").fetchdf()
```

## Files

```
walkthru-earth/indices/population/
  scenario=SSP2/
    h3_res=1/data.parquet        40 KB          430 cells
    h3_res=2/data.parquet       179 KB        2,222 cells
    h3_res=3/data.parquet       968 KB       12,310 cells
    h3_res=4/data.parquet       5.6 MB       71,552 cells
    h3_res=5/data.parquet        32 MB      414,388 cells
    h3_res=6/data.parquet       176 MB    2,288,660 cells
    h3_res=7/data.parquet       873 MB   11,421,958 cells
    h3_res=8/data.parquet       3.4 GB   44,888,216 cells
    _metadata.json
```

Compression: ZSTD level 3. Row groups: 1,000,000 rows.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `h3_index` | VARCHAR | H3 cell ID (hex string) |
| `geometry` | GEOMETRY | Cell center point (native Parquet 2.11+ GEOMETRY, EPSG:4326) |
| `lat` | FLOAT | Cell center latitude (degrees) |
| `lon` | FLOAT | Cell center longitude (degrees) |
| `area_km2` | FLOAT | H3 cell area (km²) |
| `pop_2025` | FLOAT | Projected population count, 2025 |
| `pop_2030` | FLOAT | Projected population count, 2030 |
| `pop_2035` | FLOAT | Projected population count, 2035 |
| `pop_2040` | FLOAT | Projected population count, 2040 |
| `pop_2045` | FLOAT | Projected population count, 2045 |
| `pop_2050` | FLOAT | Projected population count, 2050 |
| `pop_2055` | FLOAT | Projected population count, 2055 |
| `pop_2060` | FLOAT | Projected population count, 2060 |
| `pop_2065` | FLOAT | Projected population count, 2065 |
| `pop_2070` | FLOAT | Projected population count, 2070 |
| `pop_2075` | FLOAT | Projected population count, 2075 |
| `pop_2080` | FLOAT | Projected population count, 2080 |
| `pop_2085` | FLOAT | Projected population count, 2085 |
| `pop_2090` | FLOAT | Projected population count, 2090 |
| `pop_2095` | FLOAT | Projected population count, 2095 |
| `pop_2100` | FLOAT | Projected population count, 2100 |

**Sample values** (res 5, most populated cells, SSP2 2025):

| h3_index | lat | lon | pop_2025 | pop_2050 | pop_2100 |
|----------|-----|-----|----------|----------|----------|
| 855c9903fffffff | +28.6 | +77.2 | 9,834,719 | 10,512,443 | 8,914,326 |
| 8544ad4bfffffff | +23.1 | +72.6 | 7,126,541 | 8,021,334 | 7,432,105 |

## How It Works

1. WorldPop 1 km rasters are read in 5° × 5° geographic windows
2. Each pixel is assigned to an H3 cell via `h3.latlng_to_cell()`
3. Population counts are **summed** per H3 cell (correct for count data)
4. Overlapping window-boundary cells are deduplicated via `GROUP BY h3_index, SUM()`
5. Final Parquet is sorted by `h3_index` with native GEOMETRY via DuckDB spatial

All resolutions produce consistent world totals (~8.19 billion for 2025).

## More Examples

```sql
-- Population growth hotspots (2025 → 2100)
SELECT h3_index, lat, lon,
       pop_2025, pop_2100,
       (pop_2100 / pop_2025)::FLOAT AS growth_ratio
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/scenario=SSP2/h3_res=5/data.parquet')
WHERE pop_2025 > 100000
ORDER BY growth_ratio DESC
LIMIT 20;

-- Continental totals
SELECT CASE
         WHEN lat BETWEEN -35 AND 37 AND lon BETWEEN -20 AND 55 THEN 'Africa'
         WHEN lat BETWEEN 5 AND 55 AND lon BETWEEN 60 AND 150 THEN 'Asia'
         WHEN lat BETWEEN 35 AND 72 AND lon BETWEEN -12 AND 45 THEN 'Europe'
         WHEN lat BETWEEN -56 AND 15 AND lon BETWEEN -82 AND -34 THEN 'South America'
         WHEN lat BETWEEN 15 AND 72 AND lon BETWEEN -170 AND -50 THEN 'North America'
         ELSE 'Other'
       END AS continent,
       SUM(pop_2025)::BIGINT AS pop_2025,
       SUM(pop_2050)::BIGINT AS pop_2050,
       SUM(pop_2100)::BIGINT AS pop_2100
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/scenario=SSP2/h3_res=5/data.parquet')
GROUP BY continent
ORDER BY pop_2025 DESC;

-- DuckDB-WASM (browser) — use HTTPS URL
SELECT h3_index, pop_2025, pop_2050
FROM read_parquet(
    'https://data.source.coop/walkthru-earth/indices/population/scenario=SSP2/h3_res=5/data.parquet'
)
WHERE lat BETWEEN 35 AND 45 AND lon BETWEEN -10 AND 5
LIMIT 100;
```

## Geometry Format

The `geometry` column uses the [native Parquet 2.11+ GEOMETRY logical type](https://github.com/apache/parquet-format/blob/master/Geospatial.md) with GeoParquet 1.0 file-level metadata for backwards compatibility (`GEOPARQUET_VERSION 'BOTH'`). DuckDB 1.5+ writes per-row-group bounding box statistics automatically.

Supported by: DuckDB 1.5+, Apache Arrow (Rust), Apache Iceberg, GDAL 3.12+.

## Source

[WorldPop Global SSP Projections v0.2](https://www.worldpop.org/) — 30 arc-second (~1 km) gridded population projections for 2025–2100 under five Shared Socioeconomic Pathways (SSP1–SSP5). Float32, LZW-compressed GeoTIFF, EPSG:4326, coverage 60°S–84°N.

> WorldPop. (2018). Global 1km-grid population projections, v0.2. University of Southampton. [doi:10.5258/SOTON/WP00849](https://doi.org/10.5258/SOTON/WP00849)

## License

This dataset is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links). The source [WorldPop SSP Projections](https://doi.org/10.5258/SOTON/WP00849) is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by the [University of Southampton](https://www.worldpop.org/).
