"""Measure how much DuckDB compresses one fiscal year of USAspending contract
transactions at several grains, with explicit typing.

  1. full_auto   - every column, every transaction row, types auto-detected
  2. trimmed     - transaction rows, labor-relevant columns, EXPLICIT types
                   (DECIMAL money, DATE dates, BOOLEAN flags, ENUM codes)
  3. award       - one row per prime award (latest modification), typed
  4. recipient   - recipient x awarding agency aggregates

Usage: python scripts/compression_test.py raw/FY2024_All_Contracts_Full_*.csv
"""
import glob
import os
import sys
import time

import duckdb

csvs = sorted(sum((glob.glob(p) for p in sys.argv[1:]), []))
assert csvs, "no csv files matched"
out = "scratch"
os.makedirs(out, exist_ok=True)

csv_bytes = sum(os.path.getsize(f) for f in csvs)
print(f"{len(csvs)} csv files, {csv_bytes/2**30:.2f} GB uncompressed")

# Column -> type for the trimmed table. Everything not listed is VARCHAR
# (DuckDB dictionary-encodes those). ENUM for known-small code sets.
MONEY = "DECIMAL(18,2)"
TYPED = {
    "contract_transaction_unique_key": "VARCHAR",
    "contract_award_unique_key": "VARCHAR",
    "award_id_piid": "VARCHAR",
    "modification_number": "VARCHAR",
    "parent_award_id_piid": "VARCHAR",
    "federal_action_obligation": MONEY,
    "current_total_value_of_award": MONEY,
    "potential_total_value_of_award": MONEY,
    "action_date": "DATE",
    "period_of_performance_start_date": "DATE",
    "period_of_performance_current_end_date": "DATE",
    "period_of_performance_potential_end_date": "DATE",
    "awarding_agency_code": "VARCHAR",
    "awarding_agency_name": "VARCHAR",
    "awarding_sub_agency_code": "VARCHAR",
    "awarding_sub_agency_name": "VARCHAR",
    "funding_agency_code": "VARCHAR",
    "funding_agency_name": "VARCHAR",
    "recipient_uei": "VARCHAR",
    "recipient_name": "VARCHAR",
    "recipient_name_raw": "VARCHAR",
    "recipient_parent_uei": "VARCHAR",
    "recipient_parent_name": "VARCHAR",
    "recipient_address_line_1": "VARCHAR",
    "recipient_city_name": "VARCHAR",
    "recipient_county_name": "VARCHAR",
    "recipient_state_code": "VARCHAR",
    "recipient_zip_4_code": "VARCHAR",
    "primary_place_of_performance_city_name": "VARCHAR",
    "primary_place_of_performance_county_name": "VARCHAR",
    "primary_place_of_performance_state_code": "VARCHAR",
    "primary_place_of_performance_zip_4": "VARCHAR",
    "award_or_idv_flag": "VARCHAR",
    "award_type_code": "VARCHAR",
    "award_type": "VARCHAR",
    "type_of_contract_pricing_code": "VARCHAR",
    "type_of_contract_pricing": "VARCHAR",
    "transaction_description": "VARCHAR",
    "prime_award_base_transaction_description": "VARCHAR",
    "naics_code": "VARCHAR",
    "naics_description": "VARCHAR",
    "product_or_service_code": "VARCHAR",
    "product_or_service_code_description": "VARCHAR",
    "labor_standards_code": "VARCHAR",
    "labor_standards": "VARCHAR",
    "construction_wage_rate_requirements_code": "VARCHAR",
    "construction_wage_rate_requirements": "VARCHAR",
    "materials_supplies_articles_equipment_code": "VARCHAR",
    "materials_supplies_articles_equipment": "VARCHAR",
    "type_of_set_aside_code": "VARCHAR",
    "type_of_set_aside": "VARCHAR",
    "contracting_officers_determination_of_business_size": "VARCHAR",
    "small_disadvantaged_business": "BOOLEAN",
    "woman_owned_business": "BOOLEAN",
    "veteran_owned_business": "BOOLEAN",
    "minority_owned_business": "BOOLEAN",
    "labor_surplus_area_firm": "BOOLEAN",
    "nonprofit_organization": "BOOLEAN",
    "last_modified_date": "DATE",
}
# Low-cardinality columns worth promoting from VARCHAR to ENUM.
ENUMS = [
    "awarding_agency_code", "awarding_agency_name",
    "awarding_sub_agency_code", "awarding_sub_agency_name",
    "funding_agency_code", "funding_agency_name",
    "recipient_state_code", "primary_place_of_performance_state_code",
    "award_or_idv_flag", "award_type_code", "award_type",
    "type_of_contract_pricing_code", "type_of_contract_pricing",
    "naics_code", "naics_description",
    "product_or_service_code", "product_or_service_code_description",
    "labor_standards_code", "labor_standards",
    "construction_wage_rate_requirements_code", "construction_wage_rate_requirements",
    "materials_supplies_articles_equipment_code", "materials_supplies_articles_equipment",
    "type_of_set_aside_code", "type_of_set_aside",
    "contracting_officers_determination_of_business_size",
]


def size(path):
    return os.path.getsize(path)


def compact(path, name):
    """Copy the table into a fresh file so we measure real storage, not slack."""
    con = duckdb.connect()
    con.execute(f"ATTACH '{path}' AS a (READ_ONLY)")
    con.execute(f"ATTACH '{path}.compact' AS b")
    con.execute(f"CREATE TABLE b.{name} AS SELECT * FROM a.{name}")
    con.close()
    os.replace(f"{path}.compact", path)


def report(name, path, n, t):
    print(f"{name:10s} {n:>12,} rows  {size(path)/2**20:>8.0f} MB  "
          f"{size(path)/csv_bytes:6.1%} of csv  ({time.time()-t:.0f}s)", flush=True)


def run(name, sql, src_db=None, pre=()):
    path = f"{out}/{name}.duckdb"
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    con.execute("SET preserve_insertion_order=false")
    if src_db:
        con.execute(f"ATTACH '{src_db}' AS src (READ_ONLY)")
    t = time.time()
    for p in pre:
        con.execute(p)
    con.execute(sql)
    n = con.execute(f"select count(*) from {name}").fetchone()[0]
    con.execute("CHECKPOINT")
    con.close()
    compact(path, name)
    report(name, path, n, t)
    return path


files = "[" + ", ".join(repr(f) for f in csvs) + "]"

# --- 1. full, auto-detected types ------------------------------------------
full = run("full_auto",
           f"CREATE TABLE full_auto AS SELECT * FROM read_csv({files}, header=true, "
           f"sample_size=-1, union_by_name=true, ignore_errors=true)")
con = duckdb.connect(full, read_only=True)
types = con.execute("select data_type, count(*) from information_schema.columns "
                    "where table_name='full_auto' group by 1 order by 2 desc").fetchall()
print("  auto-detected types:", ", ".join(f"{t}={c}" for t, c in types))
detected = dict(con.execute("select column_name, data_type from information_schema.columns "
                            "where table_name='full_auto'").fetchall())
mismatch = [(c, detected.get(c), t) for c, t in TYPED.items() if detected.get(c, "").split("(")[0] != t.split("(")[0]]
print("  trimmed cols where auto != explicit:")
for c, d, t in mismatch:
    print(f"    {c:55s} auto={d:12s} explicit={t}")
con.close()

# --- 2. trimmed, explicit types (cast from the auto table) -------------------
def cast(col, typ):
    if typ == "BOOLEAN":
        return f"(CASE {col}::VARCHAR WHEN 't' THEN true WHEN 'f' THEN false END) AS {col}"
    return f"TRY_CAST({col} AS {typ}) AS {col}"

select = ",\n    ".join(cast(c, t) for c, t in TYPED.items())
trimmed = run("trimmed", f"CREATE TABLE trimmed AS SELECT {select} FROM src.full_auto",
              src_db=full)

# --- 2b. trimmed + ENUMs ------------------------------------------------------
pre = [f"CREATE TYPE e_{c} AS ENUM (SELECT DISTINCT {c} FROM src.trimmed WHERE {c} IS NOT NULL)"
       for c in ENUMS]
select = ",\n    ".join(f"{c}::e_{c} AS {c}" if c in ENUMS else c for c in TYPED)
trimmed_e = run("trimmed_e", f"CREATE TABLE trimmed_e AS SELECT {select} FROM src.trimmed",
                src_db=trimmed, pre=pre)

# --- 3. one row per award (latest modification) ------------------------------
award = run("award", f"""
    CREATE TABLE award AS
    SELECT * EXCLUDE (contract_transaction_unique_key, modification_number,
                      federal_action_obligation, action_date, rn)
    FROM (
      SELECT *,
             row_number() OVER (PARTITION BY contract_award_unique_key
                                ORDER BY action_date DESC, modification_number DESC) AS rn,
             sum(federal_action_obligation) OVER (PARTITION BY contract_award_unique_key) AS fy_obligation
      FROM src.trimmed_e
    ) WHERE rn = 1
""", src_db=trimmed_e)

# --- 4. recipient x agency aggregates ----------------------------------------
recipient = run("recipient", """
    CREATE TABLE recipient AS
    SELECT recipient_uei, any_value(recipient_name) AS recipient_name,
           recipient_parent_uei, any_value(recipient_parent_name) AS recipient_parent_name,
           any_value(recipient_state_code) AS recipient_state_code,
           awarding_agency_code, any_value(awarding_agency_name) AS awarding_agency_name,
           count(*) AS n_transactions,
           count(distinct contract_award_unique_key) AS n_awards,
           sum(federal_action_obligation) AS obligation
    FROM src.trimmed_e
    GROUP BY recipient_uei, recipient_parent_uei, awarding_agency_code
""", src_db=trimmed_e)

# --- per-column cost in the typed transaction table ---------------------------
con = duckdb.connect(trimmed_e, read_only=True)
print("\ncolumn storage in `trimmed_e` (segments ~ 256KB each; compression used):")
rows = con.execute("""
    SELECT column_name, count(*) AS segments,
           string_agg(distinct compression, ',' ORDER BY compression) AS compression
    FROM pragma_storage_info('trimmed_e')
    WHERE segment_type != 'VALIDITY'
    GROUP BY column_name ORDER BY segments DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]:55s} {r[1]:>6} segs  {r[2]}")
