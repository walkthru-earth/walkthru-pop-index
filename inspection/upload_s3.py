"""Upload output Parquet files to S3.

Usage:
    uv run python inspection/upload_s3.py
    uv run python inspection/upload_s3.py --dry-run
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload population output to S3")
    parser.add_argument(
        "--output-dir",
        default="/data/scratch/pop/output/population",
        help="Local output directory",
    )
    parser.add_argument(
        "--bucket",
        default="us-west-2.opendata.source.coop",
    )
    parser.add_argument(
        "--prefix",
        default="walkthru-earth/indices/population",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if not output_dir.exists():
        print(f"Output dir not found: {output_dir}")
        return

    s3 = boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-west-2"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    files = sorted(output_dir.rglob("*"))
    files = [f for f in files if f.is_file()]

    print(f"Uploading {len(files)} files to s3://{args.bucket}/{args.prefix}/")

    for f in files:
        rel = f.relative_to(output_dir)
        key = f"{args.prefix}/{rel}"
        size_mb = f.stat().st_size / 1e6

        content_type = "application/octet-stream"
        if f.suffix == ".json":
            content_type = "application/json"
        elif f.suffix == ".parquet":
            content_type = "application/octet-stream"

        if args.dry_run:
            print(f"  [DRY RUN] {rel} ({size_mb:.1f} MB) -> s3://{args.bucket}/{key}")
        else:
            print(f"  Uploading {rel} ({size_mb:.1f} MB)...", end="", flush=True)
            s3.upload_file(
                str(f),
                args.bucket,
                key,
                ExtraArgs={"ContentType": content_type},
            )
            print(" done")

    print(f"\nAll {len(files)} files uploaded to s3://{args.bucket}/{args.prefix}/")


if __name__ == "__main__":
    main()
