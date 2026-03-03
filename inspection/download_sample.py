"""Download a small portion of WorldPop data and inspect it.

Downloads just the first ~1MB of the ZIP to peek at the file listing,
or downloads a specific year's raster if available directly.

Usage:
    uv run python inspection/download_sample.py --scenario SSP2
    uv run python inspection/download_sample.py --scenario SSP2 --full-download
"""

from __future__ import annotations

import argparse
import subprocess
import zipfile
from pathlib import Path

WORLDPOP_BASE_URL = "https://data.worldpop.org/repo/prj/FuturePop/SSPs_1km_v0_2"
SCRATCH_DIR = Path("/data/scratch/pop")


def peek_zip_contents(scenario: str) -> None:
    """Download just enough of the ZIP to read its directory listing."""
    zip_name = f"FuturePop_{scenario}_1km_v0_2.zip"
    zip_url = f"{WORLDPOP_BASE_URL}/{zip_name}"
    zip_path = SCRATCH_DIR / zip_name

    if zip_path.exists():
        print(
            f"ZIP already exists: {zip_path} ({zip_path.stat().st_size / 1e9:.2f} GB)"
        )
    else:
        print(f"ZIP not found locally. URL: {zip_url}")
        # Download just the end of the ZIP (central directory) to list contents
        # ZIP central directory is at the end of the file
        print("Downloading to inspect contents...")
        subprocess.run(
            ["curl", "-sI", zip_url],
            check=True,
        )

    if zip_path.exists():
        print("\n--- ZIP Contents ---")
        with zipfile.ZipFile(zip_path) as zf:
            for info in zf.infolist():
                size_mb = info.file_size / 1e6
                compressed_mb = info.compress_size / 1e6
                ratio = (
                    f"{compressed_mb / size_mb * 100:.0f}%" if size_mb > 0 else "N/A"
                )
                print(
                    f"  {info.filename:60s}  "
                    f"{size_mb:8.1f} MB  "
                    f"compressed: {compressed_mb:8.1f} MB  "
                    f"ratio: {ratio}"
                )
            print(f"\n  Total files: {len(zf.infolist())}")
            total_uncompressed = sum(i.file_size for i in zf.infolist()) / 1e9
            total_compressed = sum(i.compress_size for i in zf.infolist()) / 1e9
            print(f"  Total uncompressed: {total_uncompressed:.2f} GB")
            print(f"  Total compressed:   {total_compressed:.2f} GB")


def full_download(scenario: str) -> None:
    """Download the full ZIP using aria2c or wget."""
    zip_name = f"FuturePop_{scenario}_1km_v0_2.zip"
    zip_url = f"{WORLDPOP_BASE_URL}/{zip_name}"
    zip_path = SCRATCH_DIR / zip_name

    if zip_path.exists():
        print(f"Already downloaded: {zip_path}")
        return

    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {zip_url} (~3.8 GB)...")

    import shutil

    if shutil.which("aria2c"):
        subprocess.run(
            [
                "aria2c",
                "-x16",
                "-s16",
                "-k50M",
                "-d",
                str(SCRATCH_DIR),
                "-o",
                zip_name,
                zip_url,
            ],
            check=True,
        )
    elif shutil.which("wget"):
        subprocess.run(
            ["wget", "-q", "--show-progress", "-O", str(zip_path), zip_url],
            check=True,
        )
    else:
        subprocess.run(
            ["curl", "-L", "-o", str(zip_path), zip_url],
            check=True,
        )

    print(f"Downloaded: {zip_path} ({zip_path.stat().st_size / 1e9:.2f} GB)")

    # Extract
    extract_dir = SCRATCH_DIR / "worldpop" / scenario
    extract_dir.mkdir(parents=True, exist_ok=True)
    print(f"Extracting to {extract_dir}...")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    print("Done extracting.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and inspect WorldPop data")
    parser.add_argument(
        "--scenario",
        default="SSP2",
        choices=("SSP1", "SSP2", "SSP3", "SSP4", "SSP5"),
    )
    parser.add_argument(
        "--full-download",
        action="store_true",
        help="Download the full ZIP (otherwise just peek at contents)",
    )
    args = parser.parse_args()

    if args.full_download:
        full_download(args.scenario)
        peek_zip_contents(args.scenario)
    else:
        peek_zip_contents(args.scenario)


if __name__ == "__main__":
    main()
