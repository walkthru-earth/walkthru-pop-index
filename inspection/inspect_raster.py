"""Inspect WorldPop GeoTIFF files: metadata, stats, pixel sampling.

Usage:
    uv run python inspection/inspect_raster.py /data/scratch/pop/worldpop/SSP2/
    uv run python inspection/inspect_raster.py /path/to/specific_file.tif
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import rasterio


def inspect_file(path: Path) -> None:
    """Print comprehensive metadata and statistics for a GeoTIFF."""
    print(f"\n{'=' * 70}")
    print(f"FILE: {path.name}")
    print(f"PATH: {path}")
    print(f"SIZE: {path.stat().st_size / 1e9:.2f} GB")
    print(f"{'=' * 70}")

    with rasterio.open(path) as src:
        print("\n--- Metadata ---")
        print(f"  Driver:      {src.driver}")
        print(f"  Dimensions:  {src.width} x {src.height} px")
        print(f"  Bands:       {src.count}")
        print(f"  CRS:         {src.crs}")
        print(f"  Bounds:      {src.bounds}")
        print(f"  Transform:   {src.transform}")
        print(f"  Dtype:       {src.dtypes}")
        print(f"  Nodata:      {src.nodata}")
        print(f"  Compression: {src.compression}")
        print(f"  Interleave:  {src.interleaving}")
        print(f"  Block shape: {src.block_shapes}")

        # Band descriptions
        if src.descriptions:
            print("\n--- Band Descriptions ---")
            for i, desc in enumerate(src.descriptions, 1):
                print(f"  Band {i}: {desc or '(none)'}")

        # Scale/offset
        if src.scales:
            print(f"  Scales:  {src.scales}")
        if src.offsets:
            print(f"  Offsets: {src.offsets}")

        # Tags / metadata
        tags = src.tags()
        if tags:
            print("\n--- Tags ---")
            for k, v in tags.items():
                print(f"  {k}: {v}")

        # Overviews
        overviews = src.overviews(1)
        if overviews:
            print("\n--- Overviews (Band 1) ---")
            print(f"  Levels: {overviews}")

        # Sample statistics from center and corners
        print("\n--- Pixel Sampling ---")
        cx, cy = src.width // 2, src.height // 2
        sample_points = [
            ("Center", cy, cx),
            ("Top-left", 10, 10),
            ("Top-right", 10, src.width - 10),
            ("Bot-left", src.height - 10, 10),
            ("Bot-right", src.height - 10, src.width - 10),
        ]

        for label, row, col in sample_points:
            try:
                lon, lat = src.xy(row, col)
                vals = [
                    src.read(b, window=rasterio.windows.Window(col, row, 1, 1))[0, 0]
                    for b in range(1, min(src.count + 1, 4))  # first 3 bands
                ]
                print(f"  {label:10s} (lat={lat:+8.3f}, lon={lon:+9.3f}): {vals}")
            except Exception as e:
                print(f"  {label:10s}: error: {e}")

        # Quick band stats (windowed read of a sample region)
        print("\n--- Band Statistics (sampled 1000x1000 center window) ---")
        win_size = min(1000, src.height, src.width)
        win = rasterio.windows.Window(
            cx - win_size // 2, cy - win_size // 2, win_size, win_size
        )
        for b in range(1, min(src.count + 1, 6)):  # first 5 bands
            data = src.read(b, window=win).astype(np.float64)
            valid = data[data != src.nodata] if src.nodata is not None else data
            valid = valid[np.isfinite(valid)]
            if len(valid) > 0:
                print(
                    f"  Band {b}: min={valid.min():.2f}  max={valid.max():.2f}  "
                    f"mean={valid.mean():.2f}  std={valid.std():.2f}  "
                    f"valid={len(valid)}/{data.size} ({100 * len(valid) / data.size:.1f}%)"
                )
            else:
                print(f"  Band {b}: all nodata/invalid")

    # Also try gdalinfo if available
    import shutil

    if shutil.which("gdalinfo"):
        print("\n--- gdalinfo ---")
        import subprocess

        result = subprocess.run(
            ["gdalinfo", "-stats", "-json", str(path)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            import json

            info = json.loads(result.stdout)
            print(f"  Size:        {info.get('size', 'N/A')}")
            print(
                f"  CRS (EPSG):  {info.get('coordinateSystem', {}).get('wkt', 'N/A')[:80]}..."
            )
            bands = info.get("bands", [])
            for band in bands[:5]:
                print(
                    f"  Band {band.get('band', '?')}: "
                    f"type={band.get('type', 'N/A')}, "
                    f"nodata={band.get('noDataValue', 'N/A')}, "
                    f"desc={band.get('description', 'N/A')}"
                )
        else:
            print(f"  gdalinfo failed: {result.stderr[:200]}")


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <path-to-tif-or-directory>")
        sys.exit(1)

    target = Path(sys.argv[1])

    if target.is_file() and target.suffix.lower() in (".tif", ".tiff"):
        inspect_file(target)
    elif target.is_dir():
        tifs = sorted(target.rglob("*.tif"))
        if not tifs:
            print(f"No .tif files found in {target}")
            sys.exit(1)
        print(f"Found {len(tifs)} GeoTIFF files in {target}")
        for tif in tifs:
            inspect_file(tif)
    else:
        print(f"Not a .tif file or directory: {target}")
        sys.exit(1)


if __name__ == "__main__":
    main()
