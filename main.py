"""Global WorldPop Population to H3-indexed Parquet pipeline.

Converts WorldPop 1km SSP population projections (2025-2100) to H3-indexed,
partitioned native Parquet files with population counts per cell.

Optimized for high-core-count machines (100+ threads). Uses multiprocessing
for true CPU parallelism across geographic windows.

Output follows the walkthru-earth index format:
  population/scenario={ssp}/h3_res={N}/data.parquet

Each Parquet file contains:
  h3_index   - H3 cell ID (hex string)
  geometry   - Cell center as POINT, native Parquet 2.11+ GEOMETRY('EPSG:4326')
  lat, lon   - Cell center coordinates (float32)
  area_km2   - H3 cell area in km2 (float32)
  pop_{year} - Projected population count per cell (float32)

Usage:
    uv run main.py                              # Full global, SSP2, res 1-8
    uv run main.py --scenario SSP1              # Different scenario
    uv run main.py --resolutions 5,6,7          # Specific resolutions
    uv run main.py --workers 178                # Explicit worker count
    uv run main.py --scratch-dir /data/scratch  # Override scratch directory
    uv run main.py --dry-run                    # Preview without processing
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import re
import resource
import shutil
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb
import h3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterio.windows import from_bounds
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _mem_gb() -> str:
    """Current RSS memory in GB (for log lines)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return f"{rss / 1e9:.1f}GB"
    return f"{rss / 1e6:.1f}GB"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORLDPOP_BASE_URL = "https://data.worldpop.org/repo/prj/FuturePop/SSPs_1km_v0_2"

# Processing grid
WINDOW_SIZE = 5.0  # degrees per window
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0

# Paths (overridable via CLI or env)
SCRATCH_DIR = Path(os.environ.get("SCRATCH_DIR", "/data/scratch/pop"))
CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

# S3 output (optional)
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")
AWS_REGION = os.environ.get("AWS_REGION", "") or os.environ.get(
    "AWS_DEFAULT_REGION", "us-east-1"
)


# ---------------------------------------------------------------------------
# Download & extraction
# ---------------------------------------------------------------------------


def download_and_extract(scenario: str, scratch_dir: Path) -> Path:
    """Download and extract WorldPop SSP scenario ZIP.

    Returns the directory containing extracted GeoTIFF files.
    """
    zip_name = f"FuturePop_{scenario}_1km_v0_2.zip"
    zip_url = f"{WORLDPOP_BASE_URL}/{zip_name}"
    zip_path = scratch_dir / zip_name
    extract_dir = scratch_dir / "worldpop" / scenario

    if not zip_path.exists():
        log.info("Downloading %s (~3.8 GB)...", zip_url)
        scratch_dir.mkdir(parents=True, exist_ok=True)

        # WorldPop server does NOT support HTTP range requests,
        # so aria2c multi-segment fails. Use wget or curl instead.
        if shutil.which("wget"):
            subprocess.run(
                ["wget", "-q", "--show-progress", "-O", str(zip_path), zip_url],
                check=True,
            )
        else:
            subprocess.run(
                ["curl", "-L", "-o", str(zip_path), zip_url],
                check=True,
            )

        log.info("Downloaded: %.1f GB", zip_path.stat().st_size / 1e9)
    else:
        log.info(
            "ZIP already present: %s (%.1f GB)", zip_path, zip_path.stat().st_size / 1e9
        )

    if not extract_dir.exists() or not any(extract_dir.rglob("*.tif")):
        log.info("Extracting to %s ...", extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)
        n_files = sum(1 for _ in extract_dir.rglob("*"))
        log.info("Extracted %d files", n_files)
    else:
        log.info("Already extracted: %s", extract_dir)

    return extract_dir


# ---------------------------------------------------------------------------
# Raster discovery
# ---------------------------------------------------------------------------


def discover_rasters(extract_dir: Path) -> list[dict]:
    """Find GeoTIFFs and map them to years.

    Returns list of {year, path, band} dicts, sorted by year.
    Handles both separate-file-per-year and multi-band layouts.
    """
    tif_files = sorted(extract_dir.rglob("*.tif"))
    if not tif_files:
        raise FileNotFoundError(f"No .tif files found in {extract_dir}")

    results: list[dict] = []

    # Try: separate files per year (most common WorldPop layout)
    for tif in tif_files:
        m = re.search(r"(20[2-9]\d|21\d{2})", tif.stem)
        if m:
            results.append({"year": int(m.group(1)), "path": str(tif), "band": 1})

    if not results:
        # Fallback: single multi-band file
        tif = tif_files[0]
        with rasterio.open(tif) as src:
            for i in range(1, src.count + 1):
                desc = ""
                if src.descriptions and i - 1 < len(src.descriptions):
                    desc = src.descriptions[i - 1] or ""
                m = re.search(r"(20[2-9]\d|21\d{2})", desc)
                year = int(m.group(1)) if m else 2020 + (i - 1) * 5
                results.append({"year": year, "path": str(tif), "band": i})

    results.sort(key=lambda x: x["year"])
    for r in results:
        log.info("  year=%d  band=%d  %s", r["year"], r["band"], Path(r["path"]).name)
    log.info("Discovered %d year-rasters", len(results))
    return results


# ---------------------------------------------------------------------------
# Window generation
# ---------------------------------------------------------------------------


def generate_windows() -> list[dict]:
    """Generate non-overlapping 5x5 degree geographic windows."""
    windows: list[dict] = []
    lon = LON_MIN
    while lon < LON_MAX:
        lon_end = min(lon + WINDOW_SIZE, LON_MAX)
        lat = LAT_MIN
        while lat < LAT_MAX:
            lat_end = min(lat + WINDOW_SIZE, LAT_MAX)
            windows.append(
                {
                    "id": f"w_{lon:+08.1f}_{lat:+07.1f}",
                    "bbox": (lon, lat, lon_end, lat_end),
                }
            )
            lat = lat_end
        lon = lon_end
    return windows


# ---------------------------------------------------------------------------
# Worker: parallel window processing
# ---------------------------------------------------------------------------


def _process_window(task: dict) -> tuple[str, str, dict[int, int]]:
    """Process one geographic window for all H3 resolutions.

    For each valid pixel: assign H3 cell, then groupby-sum population per cell.
    Handles window-boundary duplicates via final DuckDB merge.

    Returns (win_id, status, {h3_res: n_unique_cells}).
    """
    win_id: str = task["win_id"]
    bbox: tuple = task["bbox"]
    raster_info: list[dict] = task["raster_info"]
    h3_resolutions: list[int] = task["h3_resolutions"]
    temp_dir_str: str = task["temp_dir"]

    west, south, east, north = bbox

    # Limit GDAL memory per worker (avoids 180 workers each using 35 GB)
    with rasterio.Env(GDAL_CACHEMAX=256):
        # Read all years' data for this window
        year_data: dict[int, np.ndarray] = {}
        pixel_lats: np.ndarray | None = None
        pixel_lons: np.ndarray | None = None
        nodata_val: float | None = None

        for info in raster_info:
            year, path, band = info["year"], info["path"], info["band"]
            try:
                with rasterio.open(path) as src:
                    win = from_bounds(west, south, east, north, src.transform)
                    data = src.read(
                        band,
                        window=win,
                        boundless=True,
                        fill_value=src.nodata if src.nodata is not None else -99999,
                    ).astype(np.float32)

                    if nodata_val is None:
                        nodata_val = src.nodata

                    if pixel_lats is None:
                        h, w = data.shape
                        if h == 0 or w == 0:
                            return win_id, "skipped_empty", {}
                        transform = src.window_transform(win)
                        rr, cc = np.meshgrid(np.arange(h), np.arange(w), indexing="ij")
                        xs, ys = rasterio.transform.xy(
                            transform, rr.ravel(), cc.ravel()
                        )
                        pixel_lons = np.asarray(xs, dtype=np.float64)
                        pixel_lats = np.asarray(ys, dtype=np.float64)

                    year_data[year] = data.ravel()
            except Exception as e:
                return win_id, f"error_read: {e}", {}

    if not year_data or pixel_lats is None:
        return win_id, "skipped_no_data", {}

    # Build valid pixel mask: finite, not nodata, at least one year has pop > 0
    mask = np.ones(len(pixel_lats), dtype=bool)
    for vals in year_data.values():
        if nodata_val is not None:
            mask &= vals != nodata_val
        mask &= np.isfinite(vals)

    any_pop = np.zeros(len(pixel_lats), dtype=bool)
    for vals in year_data.values():
        any_pop |= vals > 0
    mask &= any_pop

    n_valid = int(mask.sum())
    if n_valid == 0:
        return win_id, "skipped_no_data", {}

    lats_v = pixel_lats[mask]
    lons_v = pixel_lons[mask]
    vals_v = {yr: v[mask] for yr, v in year_data.items()}

    # Free original arrays
    del year_data, pixel_lats, pixel_lons

    cells_written: dict[int, int] = {}

    for h3_res in h3_resolutions:
        # H3 cell assignment for each valid pixel
        h3_cells = np.empty(n_valid, dtype=object)
        for i in range(n_valid):
            h3_cells[i] = h3.latlng_to_cell(lats_v[i], lons_v[i], h3_res)

        # Aggregate: sum population per H3 cell via np.bincount
        unique_cells, inverse = np.unique(h3_cells, return_inverse=True)
        n_unique = len(unique_cells)

        columns: dict[str, np.ndarray] = {"h3_index": unique_cells}

        # Cell center coordinates and area
        cell_lats = np.empty(n_unique, dtype=np.float32)
        cell_lons = np.empty(n_unique, dtype=np.float32)
        cell_areas = np.empty(n_unique, dtype=np.float32)
        for j, cell_id in enumerate(unique_cells):
            lat, lon = h3.cell_to_latlng(cell_id)
            cell_lats[j] = lat
            cell_lons[j] = lon
            cell_areas[j] = h3.cell_area(cell_id, unit="km^2")
        columns["lat"] = cell_lats
        columns["lon"] = cell_lons
        columns["area_km2"] = cell_areas

        # Sum population per cell for each year
        for yr, vals in vals_v.items():
            sums = np.bincount(inverse, weights=vals, minlength=n_unique)
            columns[f"pop_{yr}"] = sums.astype(np.float32)

        # Write temp parquet
        out_dir = Path(temp_dir_str) / f"h3_res={h3_res}"
        out_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table(columns)
        pq.write_table(table, out_dir / f"{win_id}.parquet", compression="zstd")

        cells_written[h3_res] = n_unique

    return win_id, "done", cells_written


# ---------------------------------------------------------------------------
# DuckDB merge
# ---------------------------------------------------------------------------


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with spatial extension loaded."""
    log.info("Initializing DuckDB %s", duckdb.__version__)
    con = duckdb.connect()

    for ext in ("spatial",):
        try:
            con.load_extension(ext)
        except Exception:
            con.install_extension(ext)
            con.load_extension(ext)
        log.info("  Extension '%s' loaded", ext)

    if S3_BUCKET:
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if aws_key and aws_secret:
            con.sql(f"SET s3_region='{AWS_REGION}'")
            con.sql(f"SET s3_access_key_id='{aws_key}'")
            con.sql(f"SET s3_secret_access_key='{aws_secret}'")
            con.sql("SET s3_url_style='path'")
            log.info("  S3 configured: region=%s", AWS_REGION)

    return con


def merge_temp_to_final(
    con: duckdb.DuckDBPyConnection,
    temp_dir: Path,
    h3_res: int,
    year_columns: list[str],
    scenario: str,
) -> int:
    """Merge temp Parquet files into a single sorted output per resolution.

    - GROUP BY h3_index to resolve window-boundary duplicates
    - SUM population columns
    - Add native Parquet GEOMETRY via DuckDB spatial
    - Write with ZSTD compression, sorted by h3_index

    Returns total unique cell count.
    """
    res_temp = temp_dir / f"h3_res={h3_res}"
    if not res_temp.exists():
        log.warning("No temp data for H3 res %d — skipping", h3_res)
        return 0

    temp_glob = str(res_temp / "*.parquet")

    n_cells = con.sql(
        f"SELECT count(DISTINCT h3_index) FROM read_parquet('{temp_glob}')"
    ).fetchone()[0]
    if n_cells == 0:
        log.warning("No cells for H3 res %d — skipping", h3_res)
        return 0

    log.info("Merging H3 res %d: %d unique cells", h3_res, n_cells)

    # Output path
    if S3_BUCKET:
        base = f"s3://{S3_BUCKET}"
        if S3_PREFIX:
            base = f"{base}/{S3_PREFIX}"
        base = f"{base}/population"
    else:
        base = str(SCRATCH_DIR / "output" / "population")

    output_path = f"{base}/scenario={scenario}/h3_res={h3_res}/data.parquet"
    if not S3_BUCKET:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Build SUM columns
    sum_cols = ",\n                   ".join(
        f"SUM({c})::FLOAT AS {c}" for c in year_columns
    )

    t0 = time.time()
    con.sql(f"""
        COPY (
            SELECT h3_index,
                   ST_Point(
                       any_value(lon), any_value(lat)
                   )::GEOMETRY('EPSG:4326') AS geometry,
                   any_value(lat)::FLOAT AS lat,
                   any_value(lon)::FLOAT AS lon,
                   any_value(area_km2)::FLOAT AS area_km2,
                   {sum_cols}
            FROM read_parquet('{temp_glob}', hive_partitioning=false)
            GROUP BY h3_index
            ORDER BY h3_index
        ) TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
         ROW_GROUP_SIZE 1000000, GEOPARQUET_VERSION 'BOTH')
    """)

    log.info(
        "  Wrote %s (%d cells) in %.1fs",
        output_path,
        n_cells,
        time.time() - t0,
    )
    return n_cells


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


def load_checkpoint(checkpoint_path: Path) -> dict:
    """Load processing checkpoint for resume capability."""
    if checkpoint_path.exists():
        return json.loads(checkpoint_path.read_text())
    return {"completed_windows": {}}


def save_checkpoint(state: dict, checkpoint_path: Path) -> None:
    """Save processing checkpoint atomically."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = checkpoint_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(checkpoint_path)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def write_metadata(
    scenario: str,
    years: list[int],
    h3_resolutions: list[int],
    cells_per_res: dict[int, int],
    elapsed: float,
) -> None:
    """Write _metadata.json documenting the dataset."""
    meta = {
        "dataset": "population",
        "source": f"WorldPop SSP {scenario} 1km v0.2",
        "source_url": f"{WORLDPOP_BASE_URL}/FuturePop_{scenario}_1km_v0_2.zip",
        "doi": "10.5258/SOTON/WP00849",
        "crs": "EPSG:4326",
        "geometry_type": "native_parquet_2.11_geometry",
        "geometry_encoding": "WKB with GEOMETRY logical type annotation",
        "h3_resolutions": h3_resolutions,
        "scenario": scenario,
        "years": years,
        "layout": "single Parquet file per resolution, sorted by h3_index",
        "columns": {
            "h3_index": "H3 cell ID (hex string)",
            "geometry": "Cell center POINT, native Parquet 2.11+ GEOMETRY('EPSG:4326')",
            "lat": "Cell center latitude (float32)",
            "lon": "Cell center longitude (float32)",
            "area_km2": "H3 cell area in km2 (float32)",
            **{
                f"pop_{y}": f"Projected population count for {y} (float32)"
                for y in years
            },
        },
        "aggregation": "SUM — pixel population counts summed per H3 cell",
        "compression": "ZSTD level 3",
        "cells_per_resolution": {str(k): v for k, v in sorted(cells_per_res.items())},
        "processing_time_seconds": round(elapsed, 1),
        "processing_date": time.strftime("%Y-%m-%d"),
    }

    if S3_BUCKET:
        import boto3

        s3 = boto3.client("s3", region_name=AWS_REGION)
        prefix = f"{S3_PREFIX}/population" if S3_PREFIX else "population"
        key = f"{prefix}/scenario={scenario}/_metadata.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(meta, indent=2),
            ContentType="application/json",
        )
        log.info("Wrote metadata to s3://%s/%s", S3_BUCKET, key)
    else:
        path = (
            SCRATCH_DIR
            / "output"
            / "population"
            / f"scenario={scenario}"
            / "_metadata.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(meta, indent=2))
        log.info("Wrote metadata to %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="WorldPop Population -> H3 Parquet pipeline"
    )
    parser.add_argument(
        "--scenario",
        default="SSP2",
        choices=("SSP1", "SSP2", "SSP3", "SSP4", "SSP5"),
        help="SSP scenario to process (default: SSP2)",
    )
    parser.add_argument(
        "--resolutions",
        default="1,2,3,4,5,6,7,8",
        help="Comma-separated H3 resolutions (default: 1-8)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Worker process count (default: nproc - 2)",
    )
    parser.add_argument(
        "--scratch-dir",
        type=str,
        default=None,
        help="Override scratch directory (default: /data/scratch/pop)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List windows and rasters without processing",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download phase, expect data already in scratch dir",
    )
    args = parser.parse_args()

    # Override globals from args
    global SCRATCH_DIR, CHECKPOINT_FILE
    if args.scratch_dir:
        SCRATCH_DIR = Path(args.scratch_dir)
    CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

    h3_resolutions = sorted(int(r) for r in args.resolutions.split(","))
    workers = args.workers or max(1, (os.cpu_count() or 4) - 2)

    start = time.time()
    log.info("=" * 60)
    log.info("WorldPop Population -> H3 Parquet Pipeline")
    log.info("  Scenario:    %s", args.scenario)
    log.info("  Resolutions: %s", h3_resolutions)
    log.info("  Workers:     %d", workers)
    log.info("  Scratch:     %s", SCRATCH_DIR)
    log.info("  Output:      %s", f"s3://{S3_BUCKET}" if S3_BUCKET else "local")
    log.info("  Memory:      %s", _mem_gb())
    log.info("=" * 60)

    # --- Phase 1: Download & Extract ---
    if args.skip_download:
        extract_dir = SCRATCH_DIR / "worldpop" / args.scenario
        if not extract_dir.exists():
            log.error("Extract dir %s not found. Remove --skip-download.", extract_dir)
            sys.exit(1)
    else:
        extract_dir = download_and_extract(args.scenario, SCRATCH_DIR)

    # --- Phase 2: Discover rasters ---
    raster_info = discover_rasters(extract_dir)
    years = [r["year"] for r in raster_info]
    year_columns = [f"pop_{y}" for y in years]
    log.info("Years: %s (%d total)", years, len(years))

    # Quick raster metadata check
    with rasterio.open(raster_info[0]["path"]) as src:
        log.info(
            "Raster info: %dx%d px, CRS=%s, dtype=%s, nodata=%s",
            src.width,
            src.height,
            src.crs,
            src.dtypes[0],
            src.nodata,
        )
        if src.crs and not src.crs.is_geographic:
            log.warning(
                "CRS is projected (%s), not geographic. Results may be incorrect.",
                src.crs,
            )

    # --- Phase 3: Generate windows ---
    windows = generate_windows()
    log.info("Windows: %d (%g deg x %g deg)", len(windows), WINDOW_SIZE, WINDOW_SIZE)

    if args.dry_run:
        log.info(
            "Dry run: %d windows x %d years x %d resolutions",
            len(windows),
            len(years),
            len(h3_resolutions),
        )
        for w in windows[:5]:
            log.info("  %s bbox=%s", w["id"], w["bbox"])
        if len(windows) > 5:
            log.info("  ... and %d more", len(windows) - 5)
        return

    # --- Phase 4: Parallel processing ---
    checkpoint = load_checkpoint(CHECKPOINT_FILE)
    temp_dir = SCRATCH_DIR / "temp" / args.scenario
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Build task list (skip completed windows)
    completed = checkpoint.get("completed_windows", {})
    tasks = []
    for win in windows:
        if win["id"] in completed:
            continue
        tasks.append(
            {
                "win_id": win["id"],
                "bbox": win["bbox"],
                "raster_info": raster_info,
                "h3_resolutions": h3_resolutions,
                "temp_dir": str(temp_dir),
            }
        )

    log.info(
        "Processing %d windows (%d previously completed)",
        len(tasks),
        len(completed),
    )

    if tasks:
        ctx = mp.get_context("spawn")
        done_count = 0
        error_count = 0
        skip_count = 0

        with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
            futures = {pool.submit(_process_window, t): t["win_id"] for t in tasks}

            with tqdm(
                total=len(futures), desc="Processing windows", unit="win"
            ) as pbar:
                for future in as_completed(futures):
                    try:
                        win_id, status, cell_counts = future.result()
                    except Exception as exc:
                        win_id = futures[future]
                        log.error("Window %s raised: %s", win_id, exc)
                        error_count += 1
                        pbar.update(1)
                        continue

                    if status == "done":
                        checkpoint.setdefault("completed_windows", {})[win_id] = "done"
                        save_checkpoint(checkpoint, CHECKPOINT_FILE)
                        done_count += 1
                    elif "error" in status:
                        log.warning("Window %s: %s", win_id, status)
                        error_count += 1
                    else:
                        checkpoint.setdefault("completed_windows", {})[win_id] = status
                        save_checkpoint(checkpoint, CHECKPOINT_FILE)
                        skip_count += 1

                    pbar.update(1)

        log.info(
            "Window processing complete: %d done, %d skipped, %d errors",
            done_count,
            skip_count,
            error_count,
        )

    # --- Phase 5: DuckDB merge ---
    log.info("Starting DuckDB merge phase")
    con = get_duckdb_connection()

    cells_per_res: dict[int, int] = {}
    for h3_res in h3_resolutions:
        n = merge_temp_to_final(con, temp_dir, h3_res, year_columns, args.scenario)
        cells_per_res[h3_res] = n

    elapsed = time.time() - start

    # --- Phase 6: Metadata ---
    write_metadata(args.scenario, years, h3_resolutions, cells_per_res, elapsed)

    # Cleanup temp files
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
        log.info("Cleaned up temp files: %s", temp_dir)

    # --- Summary ---
    log.info("=" * 60)
    log.info("Pipeline complete in %.1f minutes", elapsed / 60)
    for res in sorted(cells_per_res):
        log.info("  H3 res %2d: %12d cells", res, cells_per_res[res])
    total = sum(cells_per_res.values())
    log.info("  Total:     %12d cells", total)
    log.info("  Peak memory: %s", _mem_gb())
    log.info("=" * 60)

    # Completion marker
    marker = SCRATCH_DIR / "COMPLETE"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(f"Completed at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")


if __name__ == "__main__":
    main()
