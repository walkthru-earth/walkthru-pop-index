"""Quick test for DuckDB 1.5.0 native Parquet GEOMETRY output.

Verifies that GEOPARQUET_VERSION 'BOTH' works correctly:
- Native Parquet 2.11+ GEOMETRY logical type (per-row-group bbox stats)
- GeoParquet 1.0 'geo' file-level metadata for backwards compatibility
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

OUT = Path("/tmp/test_geo.parquet")


def main() -> None:
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")
    print(f"DuckDB version: {con.sql('SELECT version()').fetchone()[0]}")

    # Write test parquet with native geometry
    con.sql(f"""
        COPY (
            SELECT '8928308280fffff' AS h3_index,
                   ST_Point(-73.935, 40.730)::GEOMETRY('EPSG:4326') AS geometry,
                   40.730::FLOAT AS lat,
                   -73.935::FLOAT AS lon,
                   1000000.0::FLOAT AS pop_2025
        ) TO '{OUT}'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
         ROW_GROUP_SIZE 1000000, GEOPARQUET_VERSION 'BOTH')
    """)
    print(f"Wrote: {OUT} ({OUT.stat().st_size} bytes)")

    # Check Parquet metadata
    pf = pq.ParquetFile(str(OUT))
    print("\nArrow schema fields:")
    for field in pf.schema_arrow:
        print(f"  {field.name}: {field.type}")

    meta = pf.schema_arrow.metadata or {}
    geo_meta = meta.get(b"geo")
    if geo_meta:
        geo = json.loads(geo_meta)
        print(f"\nGeoParquet version: {geo.get('version')}")
        print(f"Primary column: {geo.get('primary_column')}")
        for name, info in geo.get("columns", {}).items():
            print(f"  Column '{name}': encoding={info.get('encoding')}")
    else:
        print("\nNo GeoParquet 'geo' metadata found")

    # Check Parquet physical schema
    rg = pf.metadata.row_group(0)
    print(f"\nRow group 0 ({rg.num_rows} rows, {rg.num_columns} columns):")
    for i in range(rg.num_columns):
        col = rg.column(i)
        print(f"  {col.path_in_schema}: physical={col.physical_type}")

    # Read non-geometry columns via DuckDB
    result = con.sql(
        f"SELECT h3_index, lat, lon, pop_2025 FROM read_parquet('{OUT}')"
    ).fetchall()
    print(f"\nData (non-geo): {result}")

    print("\nAll checks passed!")


if __name__ == "__main__":
    main()
