"""Decide which (type, fiscal year) files are stale: the upstream monthly file
name differs from the `.source` marker we last uploaded to R2.

    python scripts/plan.py --bucket labordata-warehouse-staging --prefix usaspending \
        --types contracts --first-fy 2008 [--force] [--fiscal-years 2024,2025]

Prints a JSON list of {"type": ..., "fy": ...} for the GitHub Actions matrix.
Reads R2 through boto3 using the AWS_* env vars the workflow sets.
"""
import argparse
import datetime
import json
import os
import subprocess
import sys

import boto3


def latest_file(fy, award_type):
    out = subprocess.run(
        [sys.executable, "scripts/latest_file.py", str(fy), award_type],
        check=True, capture_output=True, text=True).stdout
    return out.split("\t")[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--types", default="contracts")
    ap.add_argument("--first-fy", type=int, default=2008)
    ap.add_argument("--fiscal-years", help="comma-separated subset (default: all)")
    ap.add_argument("--force", action="store_true", help="rebuild even if unchanged")
    args = ap.parse_args()

    today = datetime.date.today()
    # federal FY N runs Oct 1 N-1 .. Sep 30 N; upstream publishes the current FY too
    last_fy = today.year + (1 if today.month >= 10 else 0)
    fys = ([int(x) for x in args.fiscal_years.split(",")] if args.fiscal_years
           else range(args.first_fy, last_fy + 1))

    s3 = boto3.client("s3", endpoint_url=os.environ["AWS_ENDPOINT_URL_S3"])
    stale = []
    for award_type in args.types.split(","):
        for fy in fys:
            upstream = latest_file(fy, award_type)
            key = f"{args.prefix}/{award_type}/FY{fy}.source"
            try:
                current = s3.get_object(Bucket=args.bucket, Key=key)["Body"].read().decode().strip()
            except s3.exceptions.NoSuchKey:
                current = None
            status = "new" if current is None else ("changed" if current != upstream else "current")
            print(f"{award_type} FY{fy}: {status} ({upstream})", file=sys.stderr)
            if args.force or status != "current":
                stale.append({"type": award_type, "fy": fy})
    print(json.dumps(stale))


if __name__ == "__main__":
    main()
