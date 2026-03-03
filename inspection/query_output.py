"""Query output Parquet files with DuckDB to verify correctness.

Usage:
    uv run python inspection/query_output.py
    uv run python inspection/query_output.py --output-dir /data/scratch/pop/output
    uv run python inspection/query_output.py --s3 s3://bucket/prefix
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb


def run_queries(con: duckdb.DuckDBPyConnection, base_path: str) -> None:
    """Run diagnostic queries on the output Parquet files."""

    # Find available resolutions
    print("\n" + "=" * 70)
    print(f"OUTPUT: {base_path}")
    print("=" * 70)

    # List available files
    if base_path.startswith("s3://"):
        glob_pattern = f"{base_path}/**/data.parquet"
    else:
        files = sorted(Path(base_path).rglob("data.parquet"))
        if not files:
            print("No data.parquet files found!")
            return
        print(f"\nFound {len(files)} resolution files:")
        for f in files:
            size_mb = f.stat().st_size / 1e6
            print(f"  {f.relative_to(base_path)} ({size_mb:.1f} MB)")
        glob_pattern = str(Path(base_path) / "**" / "data.parquet")

    # Schema
    print("\n--- Schema ---")
    try:
        result = con.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=false) LIMIT 0"
        ).fetchall()
        for col_name, col_type, *_ in result:
            print(f"  {col_name:20s}  {col_type}")
    except Exception as e:
        print(f"  Error: {e}")
        return

    # Per-resolution stats
    print("\n--- Per-Resolution Statistics ---")
    for f in sorted(Path(base_path).rglob("data.parquet")):
        rel = str(f.relative_to(base_path))
        fpath = str(f) if not base_path.startswith("s3://") else f"{base_path}/{rel}"

        result = con.sql(f"""
            SELECT
                count(*) AS n_cells,
                min(lat) AS lat_min, max(lat) AS lat_max,
                min(lon) AS lon_min, max(lon) AS lon_max,
                avg(area_km2)::FLOAT AS avg_area_km2
            FROM read_parquet('{fpath}')
        """).fetchone()

        n_cells, lat_min, lat_max, lon_min, lon_max, avg_area = result
        print(f"\n  {rel}:")
        print(f"    Cells:    {n_cells:,}")
        print(f"    Lat:      {lat_min:.2f} to {lat_max:.2f}")
        print(f"    Lon:      {lon_min:.2f} to {lon_max:.2f}")
        print(f"    Avg area: {avg_area:.2f} km2")

        # Population column stats
        pop_cols = con.sql(f"""
            SELECT column_name FROM (
                DESCRIBE SELECT * FROM read_parquet('{fpath}') LIMIT 0
            ) WHERE column_name LIKE 'pop_%'
        """).fetchall()

        if pop_cols:
            for (col,) in pop_cols[:4]:  # first 4 years
                stats = con.sql(f"""
                    SELECT
                        sum({col})::BIGINT AS total_pop,
                        avg({col})::FLOAT AS avg_pop,
                        max({col})::FLOAT AS max_pop,
                        count_if({col} > 0) AS cells_with_pop
                    FROM read_parquet('{fpath}')
                """).fetchone()
                total, avg, mx, n_pop = stats
                print(
                    f"    {col}: total={total:,}  avg={avg:.1f}  "
                    f"max={mx:.0f}  cells_w_pop={n_pop:,}"
                )

    # Top populated cells (first available file)
    first_file = sorted(Path(base_path).rglob("data.parquet"))[0]
    fpath = str(first_file)

    # Get first pop column
    pop_col = con.sql(f"""
        SELECT column_name FROM (
            DESCRIBE SELECT * FROM read_parquet('{fpath}') LIMIT 0
        ) WHERE column_name LIKE 'pop_%' LIMIT 1
    """).fetchone()

    if pop_col:
        col = pop_col[0]
        print(
            f"\n--- Top 10 Most Populated Cells ({first_file.parent.name}, {col}) ---"
        )
        result = con.sql(f"""
            SELECT h3_index, lat, lon, area_km2, {col},
                   ({col} / area_km2)::FLOAT AS density_per_km2
            FROM read_parquet('{fpath}')
            ORDER BY {col} DESC
            LIMIT 10
        """).fetchall()
        print(
            f"  {'h3_index':20s} {'lat':>8s} {'lon':>9s} {'area_km2':>10s} {col:>14s} {'density':>12s}"
        )
        for row in result:
            h3_id, lat, lon, area, pop, density = row
            print(
                f"  {h3_id:20s} {lat:+8.3f} {lon:+9.3f} {area:10.2f} {pop:14,.0f} {density:12,.1f}"
            )

    # GeoParquet metadata check
    print("\n--- GeoParquet Metadata Check ---")
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(first_file))
    meta = pf.schema_arrow.metadata or {}
    geo_meta = meta.get(b"geo")
    if geo_meta:
        import json

        geo = json.loads(geo_meta)
        print(f"  GeoParquet version: {geo.get('version', 'N/A')}")
        print(f"  Primary column:    {geo.get('primary_column', 'N/A')}")
        cols = geo.get("columns", {})
        for name, info in cols.items():
            print(f"  Column '{name}': encoding={info.get('encoding', 'N/A')}")
    else:
        print("  No GeoParquet 'geo' metadata found")

    # Check for GEOMETRY logical type
    for field in pf.schema_arrow:
        if "geometry" in field.name.lower():
            print(f"  Arrow field '{field.name}': type={field.type}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Query population Parquet output")
    parser.add_argument(
        "--output-dir",
        default="/data/scratch/pop/output/population",
        help="Local output directory",
    )
    parser.add_argument(
        "--s3",
        default="",
        help="S3 URI (e.g. s3://bucket/prefix/population)",
    )
    parser.add_argument(
        "--scenario",
        default="SSP2",
        help="Scenario to query (default: SSP2)",
    )
    args = parser.parse_args()

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    if args.s3:
        base_path = f"{args.s3}/scenario={args.scenario}"
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if aws_key:
            con.sql(f"SET s3_access_key_id='{aws_key}'")
            con.sql(f"SET s3_secret_access_key='{aws_secret}'")
    else:
        base_path = f"{args.output_dir}/scenario={args.scenario}"

    run_queries(con, base_path)


if __name__ == "__main__":
    main()
