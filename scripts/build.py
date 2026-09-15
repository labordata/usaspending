"""Convert one monthly USAspending award archive zip into a typed, sorted
Parquet file.

    python scripts/build.py --type contracts --zip raw/FY2024_All_Contracts_Full.zip \
        --out contracts/FY2024.parquet

The zip holds several ~2 GB CSV members. They're extracted one at a time into
a temporary directory, loaded into an on-disk DuckDB staging table (columns
and types from columns.yml), and deleted -- so peak disk is one zip + one CSV
+ the staging table, which fits a stock GitHub Actions runner. The final COPY
sorts by recipient so Parquet row-group statistics make per-recipient lookups
cheap, and records the source file name in the Parquet key/value metadata.
"""
import argparse
import datetime
import os
import shutil
import sys
import tempfile
import zipfile

import duckdb
import yaml

SQL_TYPES = {"MONEY": "DECIMAL(18,2)"}
SORT = {
    "contracts": "recipient_uei, action_date, award_id_piid, modification_number",
    "assistance": "recipient_uei, action_date",
}


def cast(col, typ):
    q = f'"{col}"'
    if typ == "BOOLEAN":
        return f"(CASE {q} WHEN 't' THEN true WHEN 'f' THEN false END) AS {q}"
    if typ == "VARCHAR":
        return q
    if typ == "DATE":
        # some date columns carry a time and zone ('2023-11-07 17:38:05+00')
        return f"TRY_CAST(TRY_CAST({q} AS TIMESTAMP) AS DATE) AS {q}"
    return f"TRY_CAST({q} AS {SQL_TYPES.get(typ, typ)}) AS {q}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--type", required=True, choices=("contracts", "assistance"))
    ap.add_argument("--zip", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--source-name", help="upstream file name to record (default: zip basename)")
    ap.add_argument("--columns", default="columns.yml")
    ap.add_argument("--where", help="SQL predicate over the raw (all-VARCHAR) columns; rows failing it are not loaded")
    ap.add_argument("--memory-limit", default="6GB")
    ap.add_argument("--tmp", help="scratch dir for extracted CSVs + staging db (default: system temp)")
    args = ap.parse_args()

    cols = yaml.safe_load(open(args.columns))[args.type]
    source = args.source_name or os.path.basename(args.zip)
    tmp = tempfile.mkdtemp(dir=args.tmp)
    try:
        con = duckdb.connect(os.path.join(tmp, "staging.duckdb"))
        con.execute(f"SET memory_limit='{args.memory_limit}'")
        con.execute(f"SET temp_directory='{tmp}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("CREATE TABLE tx (" + ", ".join(
            f'"{c}" {SQL_TYPES.get(t, t)}' for c, t in cols.items()) + ")")

        with zipfile.ZipFile(args.zip) as zf:
            members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
            for i, member in enumerate(members, 1):
                print(f"[{i}/{len(members)}] {member}", file=sys.stderr, flush=True)
                csv_path = zf.extract(member, tmp)
                # all_varchar: no type sniffing pass; every cast is explicit below.
                read = (f"read_csv('{csv_path}', header=true, all_varchar=true, "
                        f"quote='\"', escape='\"')")
                present = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {read}").fetchall()}
                missing = [c for c in cols if c not in present]
                if missing:
                    print(f"    columns absent in this file (filled NULL): {missing}",
                          file=sys.stderr)
                select = ", ".join(
                    cast(c, t) if c in present else f'NULL::{SQL_TYPES.get(t, t)} AS "{c}"'
                    for c, t in cols.items())
                where = f" WHERE {args.where}" if args.where else ""
                con.execute(f"INSERT INTO tx SELECT {select} FROM {read}{where}")
                os.remove(csv_path)

        n = con.execute("SELECT count(*) FROM tx").fetchone()[0]
        # Casts that failed became NULL; report any typed column where that happened
        # a lot so a schema change upstream doesn't silently blank a column.
        for c, t in cols.items():
            if t == "VARCHAR":
                continue
            nulls = con.execute(f'SELECT count(*) FROM tx WHERE "{c}" IS NULL').fetchone()[0]
            if n and nulls == n:
                print(f"    warning: {c} is entirely NULL after cast to {t}", file=sys.stderr)

        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        built = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        con.execute(f"""
            COPY (SELECT * FROM tx ORDER BY {SORT[args.type]})
            TO '{args.out}'
            (FORMAT parquet, COMPRESSION zstd,
             KV_METADATA {{source: '{source}', built_at: '{built}',
                           filter: '{(args.where or "").replace("'", "''")}'}})
        """)
        con.close()
        print(f"{args.out}: {n:,} rows, {os.path.getsize(args.out)/2**20:.0f} MB, source {source}",
              file=sys.stderr)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
