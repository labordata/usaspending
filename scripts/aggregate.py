"""Build recipient-level yearly rollups from every contracts Parquet file on
R2 and write them as Parquet next to the per-year files.

    python scripts/aggregate.py --bucket labordata-warehouse-staging --prefix usaspending

DuckDB reads the remote files over httpfs and only fetches the columns the
rollup needs. Uses the AWS_* env vars the workflow sets.
"""
import argparse
import os

import duckdb

# Grain is one row per recipient UEI per fiscal year (per agency, for the
# second rollup). Everything else about the recipient -- DUNS, parent, name,
# address -- varies between that recipient's own transactions, so those are
# any_value() rather than grouping keys.
#
# USAspending ships labor_standards_code / labor_standards with the code and
# description swapped (at least in FY2024), while the other two prevailing-wage
# pairs are the right way round; accept 'Y' from either column.
ROLLUP = """
    SELECT
        action_date_fiscal_year AS fiscal_year,
        recipient_uei,
        any_value(recipient_duns) AS recipient_duns,
        any_value(recipient_name) AS recipient_name,
        any_value(recipient_parent_uei) AS recipient_parent_uei,
        any_value(recipient_parent_name) AS recipient_parent_name,
        any_value(recipient_city_name) AS recipient_city_name,
        any_value(recipient_state_code) AS recipient_state_code,
        {agency_cols}
        count(*) AS transactions,
        count(DISTINCT award_id_piid) AS awards,
        sum(federal_action_obligation) AS obligations,
        sum(federal_action_obligation)
            FILTER (WHERE labor_standards_code = 'Y' OR labor_standards = 'Y')
            AS service_contract_act_obligations,
        sum(federal_action_obligation)
            FILTER (WHERE construction_wage_rate_requirements_code = 'Y')
            AS davis_bacon_obligations,
        sum(federal_action_obligation)
            FILTER (WHERE materials_supplies_articles_equipment_code = 'Y')
            AS walsh_healey_obligations,
        max(highly_compensated_officer_1_amount) AS top_officer_compensation
    FROM read_parquet('{src}')
    GROUP BY fiscal_year, recipient_uei {agency_group}
    ORDER BY recipient_uei, fiscal_year {agency_group}
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--out-dir", default="aggregates")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE SECRET r2 (TYPE s3, PROVIDER credential_chain,
            ENDPOINT '{os.environ["AWS_ENDPOINT_URL_S3"].removeprefix("https://")}',
            REGION 'auto', URL_STYLE 'path')
    """)
    src = f"s3://{args.bucket}/{args.prefix}/contracts/FY*.parquet"
    os.makedirs(args.out_dir, exist_ok=True)

    for name, agency_cols, agency_group in [
        ("recipient_year", "", ""),
        ("recipient_year_agency",
         "awarding_agency_code, any_value(awarding_agency_name) AS awarding_agency_name,",
         ", awarding_agency_code"),
    ]:
        out = os.path.join(args.out_dir, f"{name}.parquet")
        sql = ROLLUP.format(src=src, agency_cols=agency_cols, agency_group=agency_group)
        con.execute(f"COPY ({sql}) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
        n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
        print(f"{out}: {n:,} rows, {os.path.getsize(out)/2**20:.0f} MB")


if __name__ == "__main__":
    main()
