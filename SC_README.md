# Global Population Projections (H3-indexed)

WorldPop SSP 1km population projections (2025–2100) aggregated to [H3](https://h3geo.org/) hexagonal cells in optimized Parquet format. Eight H3 resolutions (1–8), one file per resolution, sorted by `h3_index`. v2 (recommended) uses BIGINT `h3_index` with 17 columns; v1 (legacy) uses VARCHAR `h3_index` with geometry/lat/lon/area_km2.

| | |
|---|---|
| **Source** | [WorldPop](https://www.worldpop.org/) Global SSP Projections v0.2, 30 arc-second (~1 km), global coverage |
| **Format** | Apache Parquet (v2 optimized: BIGINT h3_index, no geometry columns) |
| **CRS** | EPSG:4326 (derivable via DuckDB h3 extension) |
| **License** | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links) |
| **Code** | [walkthru-earth/walkthru-pop-index](https://github.com/walkthru-earth/walkthru-pop-index) |

## Quick Start

```sql
-- DuckDB (v2, recommended)
INSTALL h3 FROM community; LOAD h3;
INSTALL httpfs;             LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index,
       h3_h3_to_string(h3_index) AS h3_hex,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       pop_2025, pop_2050, pop_2100,
       (pop_2100 / pop_2025)::FLOAT AS growth_ratio
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet')
ORDER BY pop_2025 DESC
LIMIT 20;
```

```python
# Python (v2)
import duckdb

con = duckdb.connect()
con.install_extension("httpfs"); con.load_extension("httpfs")
con.execute("INSTALL h3 FROM community"); con.load_extension("h3")
con.sql("SET s3_region = 'us-west-2'")

df = con.sql("""
    SELECT h3_index,
           h3_cell_to_lat(h3_index) AS lat,
           h3_cell_to_lng(h3_index) AS lng,
           pop_2025, pop_2050, pop_2100
    FROM read_parquet(
        's3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet'
    ) WHERE h3_cell_to_lat(h3_index) BETWEEN 20 AND 35
        AND h3_cell_to_lng(h3_index) BETWEEN 68 AND 90
""").fetchdf()
```

## Files

```
walkthru-earth/indices/population/
  v2/scenario=SSP2/                              # recommended
    h3_res=1/data.parquet        430 cells
    h3_res=2/data.parquet      2,222 cells
    h3_res=3/data.parquet     12,310 cells
    h3_res=4/data.parquet     71,552 cells
    h3_res=5/data.parquet    414,388 cells
    h3_res=6/data.parquet  2,288,660 cells
    h3_res=7/data.parquet 11,421,958 cells
    h3_res=8/data.parquet 44,888,216 cells
    _metadata.json
  v1/scenario=SSP2/                              # legacy (VARCHAR h3_index, has geometry/lat/lon/area_km2)
    h3_res=1/data.parquet .. h3_res=8/data.parquet
```

Compression: ZSTD level 3. Row groups: 1,000,000 rows.

## Schema (v2, recommended)

| Column | Type | Description |
|--------|------|-------------|
| `h3_index` | BIGINT | H3 cell ID (integer representation) |
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

17 columns total. Geometry, lat/lon, and area_km2 from v1 are derivable from `h3_index` via the DuckDB `h3` extension (e.g., `h3_cell_to_lat(h3_index)`, `h3_cell_to_lng(h3_index)`).

**Sample values** (res 5, most populated cells, SSP2 2025):

| h3_index | pop_2025 | pop_2050 | pop_2100 |
|----------|----------|----------|----------|
| 599686542969856000 | 9,834,719 | 10,512,443 | 8,914,326 |
| 599262598287056896 | 7,126,541 | 8,021,334 | 7,432,105 |

## How It Works

1. WorldPop 1 km rasters are read in 5° × 5° geographic windows
2. Each pixel is assigned to an H3 cell via `h3.latlng_to_cell()`
3. Population counts are **summed** per H3 cell (correct for count data)
4. Overlapping window-boundary cells are deduplicated via `GROUP BY h3_index, SUM()`
5. Final Parquet is sorted by `h3_index` (v2 stores BIGINT h3_index only; v1 legacy includes native GEOMETRY via DuckDB spatial)

All resolutions produce consistent world totals (~8.19 billion for 2025).

## More Examples

```sql
-- Population growth hotspots (2025 → 2100)
SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       pop_2025, pop_2100,
       (pop_2100 / pop_2025)::FLOAT AS growth_ratio
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet')
WHERE pop_2025 > 100000
ORDER BY growth_ratio DESC
LIMIT 20;

-- Continental totals
SELECT CASE
         WHEN h3_cell_to_lat(h3_index) BETWEEN -35 AND 37 AND h3_cell_to_lng(h3_index) BETWEEN -20 AND 55 THEN 'Africa'
         WHEN h3_cell_to_lat(h3_index) BETWEEN 5 AND 55 AND h3_cell_to_lng(h3_index) BETWEEN 60 AND 150 THEN 'Asia'
         WHEN h3_cell_to_lat(h3_index) BETWEEN 35 AND 72 AND h3_cell_to_lng(h3_index) BETWEEN -12 AND 45 THEN 'Europe'
         WHEN h3_cell_to_lat(h3_index) BETWEEN -56 AND 15 AND h3_cell_to_lng(h3_index) BETWEEN -82 AND -34 THEN 'South America'
         WHEN h3_cell_to_lat(h3_index) BETWEEN 15 AND 72 AND h3_cell_to_lng(h3_index) BETWEEN -170 AND -50 THEN 'North America'
         ELSE 'Other'
       END AS continent,
       SUM(pop_2025)::BIGINT AS pop_2025,
       SUM(pop_2050)::BIGINT AS pop_2050,
       SUM(pop_2100)::BIGINT AS pop_2100
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet')
GROUP BY continent
ORDER BY pop_2025 DESC;

-- DuckDB-WASM (browser) — use HTTPS URL
SELECT h3_index, pop_2025, pop_2050
FROM read_parquet(
    'https://data.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet'
)
WHERE h3_cell_to_lat(h3_index) BETWEEN 35 AND 45
  AND h3_cell_to_lng(h3_index) BETWEEN -10 AND 5
LIMIT 100;
```

## Geometry / Coordinate Derivation

v2 files do not include geometry, lat, lon, or area_km2 columns — these are derivable from the BIGINT `h3_index` using the DuckDB `h3` community extension:

```sql
INSTALL h3 FROM community; LOAD h3;

SELECT h3_index,
       h3_h3_to_string(h3_index)  AS h3_hex,
       h3_cell_to_lat(h3_index)   AS lat,
       h3_cell_to_lng(h3_index)   AS lng,
       h3_cell_area(h3_index, 'km^2') AS area_km2
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/population/v2/scenario=SSP2/h3_res=5/data.parquet')
LIMIT 5;
```

v1 (legacy) files retain the original schema with VARCHAR `h3_index`, native Parquet 2.11+ GEOMETRY, lat, lon, and area_km2 columns. v1 is located at `indices/population/v1/scenario=SSP2/h3_res={1-8}/data.parquet`.

## Source

[WorldPop Global SSP Projections v0.2](https://www.worldpop.org/) — 30 arc-second (~1 km) gridded population projections for 2025–2100 under five Shared Socioeconomic Pathways (SSP1–SSP5). Float32, LZW-compressed GeoTIFF, EPSG:4326, coverage 60°S–84°N.

> WorldPop. (2018). Global 1km-grid population projections, v0.2. University of Southampton. [doi:10.5258/SOTON/WP00849](https://doi.org/10.5258/SOTON/WP00849)

## License

This dataset is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links). The source [WorldPop SSP Projections](https://doi.org/10.5258/SOTON/WP00849) is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by the [University of Southampton](https://www.worldpop.org/).
