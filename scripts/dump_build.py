"""Build Parquet files for the tables in dump_tables.yml from USAspending's
monthly PostgreSQL dump, fetching only those tables (see dump_extract.py).

    python scripts/dump_build.py --dump latest --out-dir dump
    python scripts/dump_build.py --dump usaspending-db_20260906.zip --only subawards,naics

Each table's gzip is range-fetched next to the output and DuckDB reads it
directly (no decompressed TSV on disk: subaward_search is 4 GB gzip / 26 GB
text). The COPY terminator line becomes a one-field row under null_padding
and is filtered out. Types are sniffed by DuckDB over the whole file.
"""
import argparse
import datetime
import os
import sys

import duckdb
import yaml

sys.path.insert(0, os.path.dirname(__file__))
import dump_extract as de  # noqa: E402


def build(dump, name, spec, out_dir, con):
    gz = os.path.join(out_dir, f"{name}.dat.gz")
    columns = dump.fetch_table(spec["table"], gz)
    drop = set(spec.get("drop", []))
    missing = drop - set(columns)
    if missing:
        print(f"  note: drop list names columns not in the table: {sorted(missing)}", file=sys.stderr)
    keep = [c for c in columns if c not in drop]
    select = ", ".join(f'"{c}"' for c in keep)
    first = columns[0]
    out = os.path.join(out_dir, f"{name}.parquet")
    built = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.execute(f"""
        COPY (
            SELECT {select}
            FROM read_csv('{gz}', delim='\t', header=false, nullstr='\\N', quote='', escape='',
                          names={columns}, null_padding=true, compression='gzip', sample_size=-1)
            WHERE "{first}" IS DISTINCT FROM '\\.'
            ORDER BY {spec['sort']}
        ) TO '{out}' (FORMAT parquet, COMPRESSION zstd,
                      KV_METADATA {{source: '{dump.name}', source_table: '{spec['table']}', built_at: '{built}'}})
    """)
    os.remove(gz)
    n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
    print(f"{out}: {n:,} rows, {len(keep)} columns, {os.path.getsize(out)/2**20:.0f} MB", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default="latest")
    ap.add_argument("--tables", default="dump_tables.yml")
    ap.add_argument("--only", help="comma-separated subset of names")
    ap.add_argument("--out-dir", default="dump")
    ap.add_argument("--memory-limit", default="8GB")
    args = ap.parse_args()

    specs = yaml.safe_load(open(args.tables))
    if args.only:
        specs = {k: specs[k] for k in args.only.split(",")}
    os.makedirs(args.out_dir, exist_ok=True)
    dump = de.Dump(de.latest_dump() if args.dump == "latest" else args.dump)
    print(f"{dump.name}: {dump.size/2**30:.0f} GiB", file=sys.stderr)
    with open(os.path.join(args.out_dir, "SOURCE"), "w") as f:
        f.write(dump.name + "\n")

    con = duckdb.connect(os.path.join(args.out_dir, "scratch.duckdb"))
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"SET temp_directory='{os.path.join(args.out_dir, 'tmp')}'")
    con.execute("SET preserve_insertion_order=false")
    for name, spec in specs.items():
        build(dump, name, spec, args.out_dir, con)
    con.close()
    os.remove(os.path.join(args.out_dir, "scratch.duckdb"))


if __name__ == "__main__":
    main()
