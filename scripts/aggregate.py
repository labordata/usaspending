"""Build recipient-level rollups from every contracts Parquet file on R2 and
write them as Parquet next to the per-year files.

    python scripts/aggregate.py --bucket labordata-warehouse-staging --prefix usaspending

Two outputs:

  recipients.parquet      one row per recipient UEI: current name and address
                          (from the most recent transaction), lifetime totals,
                          top NAICS and awarding agency by dollars, business
                          type flags. The browse / facet / crosswalk surface.
  recipient_year.parquet  one row per recipient UEI per fiscal year, with
                          obligations split by prevailing-wage regime. The
                          cross-recipient time series ("top SCA contractors in
                          2019") that would otherwise need a 93M-row scan.

Per-recipient drill-downs (spending by agency, by place, award lists) are
canned queries against `contracts` instead: the Parquet is sorted by
recipient_uei, so a single-recipient filter prunes to a few row groups and
runs in well under a second.

DuckDB reads the remote files over httpfs and only fetches the columns the
rollups need. Uses the AWS_* env vars the workflow sets.
"""
import argparse
import os

import duckdb

# USAspending's August 2026 archive shipped labor_standards_code /
# labor_standards with the code and description swapped; September's has them
# the right way round. Accept 'Y' from either column so the rollup is right
# whichever vintage a fiscal year was last built from.
SCA = "(labor_standards_code = 'Y' OR labor_standards = 'Y')"
DAVIS_BACON = "(construction_wage_rate_requirements_code = 'Y')"
WALSH_HEALEY = "(materials_supplies_articles_equipment_code = 'Y')"

RECIPIENT_YEAR = f"""
    SELECT
        action_date_fiscal_year AS fiscal_year,
        recipient_uei,
        arg_max(recipient_name, action_date) AS recipient_name,
        arg_max(recipient_parent_uei, action_date) AS recipient_parent_uei,
        arg_max(recipient_parent_name, action_date) AS recipient_parent_name,
        arg_max(recipient_city_name, action_date) AS recipient_city_name,
        arg_max(recipient_state_code, action_date) AS recipient_state_code,
        count(*) AS transactions,
        count(DISTINCT award_id_piid) AS awards,
        sum(federal_action_obligation) AS obligations,
        sum(federal_action_obligation) FILTER (WHERE {SCA}) AS service_contract_act_obligations,
        sum(federal_action_obligation) FILTER (WHERE {DAVIS_BACON}) AS davis_bacon_obligations,
        sum(federal_action_obligation) FILTER (WHERE {WALSH_HEALEY}) AS walsh_healey_obligations,
        max(highly_compensated_officer_1_amount) AS top_officer_compensation
    FROM read_parquet('{{src}}')
    GROUP BY fiscal_year, recipient_uei
    ORDER BY recipient_uei, fiscal_year
"""

# "Top" NAICS / agency = the one with the most obligated dollars, lifetime.
RECIPIENTS = f"""
    WITH tx AS (
        SELECT recipient_uei, recipient_name, recipient_duns, recipient_doing_business_as_name,
               recipient_parent_uei, recipient_parent_name,
               recipient_address_line_1, recipient_city_name, recipient_county_name,
               recipient_state_code, recipient_zip_4_code, recipient_country_code,
               action_date, action_date_fiscal_year, award_id_piid,
               federal_action_obligation,
               naics_code, naics_description,
               awarding_agency_code, awarding_agency_name,
               primary_place_of_performance_state_code,
               contracting_officers_determination_of_business_size,
               nonprofit_organization, educational_institution, hospital_flag,
               for_profit_organization, us_state_government, us_local_government,
               woman_owned_business, veteran_owned_business, minority_owned_business,
               foreign_owned, highly_compensated_officer_1_amount,
               {SCA} AS sca, {DAVIS_BACON} AS davis_bacon, {WALSH_HEALEY} AS walsh_healey
        FROM read_parquet('{{src}}')
    ),
    top_naics AS (
        SELECT recipient_uei, naics_code, naics_description
        FROM (
            SELECT recipient_uei, naics_code, any_value(naics_description) AS naics_description,
                   row_number() OVER (PARTITION BY recipient_uei
                                      ORDER BY sum(federal_action_obligation) DESC) AS rn
            FROM tx WHERE naics_code IS NOT NULL
            GROUP BY recipient_uei, naics_code
        ) WHERE rn = 1
    ),
    top_agency AS (
        SELECT recipient_uei, awarding_agency_name
        FROM (
            SELECT recipient_uei, awarding_agency_name,
                   row_number() OVER (PARTITION BY recipient_uei
                                      ORDER BY sum(federal_action_obligation) DESC) AS rn
            FROM tx WHERE awarding_agency_name IS NOT NULL
            GROUP BY recipient_uei, awarding_agency_name
        ) WHERE rn = 1
    ),
    top_state AS (
        SELECT recipient_uei, primary_place_of_performance_state_code
        FROM (
            SELECT recipient_uei, primary_place_of_performance_state_code,
                   row_number() OVER (PARTITION BY recipient_uei
                                      ORDER BY sum(federal_action_obligation) DESC) AS rn
            FROM tx WHERE primary_place_of_performance_state_code IS NOT NULL
            GROUP BY recipient_uei, primary_place_of_performance_state_code
        ) WHERE rn = 1
    ),
    summary AS (
        SELECT
            recipient_uei,
            arg_max(recipient_name, action_date) AS recipient_name,
            arg_max(recipient_doing_business_as_name, action_date) AS doing_business_as,
            arg_max(recipient_duns, action_date) AS recipient_duns,
            arg_max(recipient_parent_uei, action_date) AS recipient_parent_uei,
            arg_max(recipient_parent_name, action_date) AS recipient_parent_name,
            arg_max(recipient_address_line_1, action_date) AS address,
            arg_max(recipient_city_name, action_date) AS city,
            arg_max(recipient_county_name, action_date) AS county,
            arg_max(recipient_state_code, action_date) AS state,
            arg_max(recipient_zip_4_code, action_date)[:5] AS zip,
            arg_max(recipient_country_code, action_date) AS country,
            min(action_date_fiscal_year) AS first_fiscal_year,
            max(action_date_fiscal_year) AS last_fiscal_year,
            count(*) AS transactions,
            count(DISTINCT award_id_piid) AS awards,
            sum(federal_action_obligation) AS obligations,
            sum(federal_action_obligation) FILTER (WHERE sca) AS service_contract_act_obligations,
            sum(federal_action_obligation) FILTER (WHERE davis_bacon) AS davis_bacon_obligations,
            sum(federal_action_obligation) FILTER (WHERE walsh_healey) AS walsh_healey_obligations,
            arg_max(contracting_officers_determination_of_business_size, action_date) AS business_size,
            arg_max(nonprofit_organization, action_date) AS nonprofit,
            arg_max(educational_institution, action_date) AS educational_institution,
            arg_max(hospital_flag, action_date) AS hospital,
            arg_max(for_profit_organization, action_date) AS for_profit,
            arg_max(us_state_government OR us_local_government, action_date) AS state_or_local_government,
            arg_max(woman_owned_business, action_date) AS woman_owned,
            arg_max(veteran_owned_business, action_date) AS veteran_owned,
            arg_max(minority_owned_business, action_date) AS minority_owned,
            arg_max(foreign_owned, action_date) AS foreign_owned,
            max(highly_compensated_officer_1_amount) AS top_officer_compensation
        FROM tx
        GROUP BY recipient_uei
    )
    SELECT s.*,
           n.naics_code AS top_naics_code, n.naics_description AS top_naics,
           a.awarding_agency_name AS top_awarding_agency,
           p.primary_place_of_performance_state_code AS top_place_of_performance_state
    FROM summary s
    LEFT JOIN top_naics n USING (recipient_uei)
    LEFT JOIN top_agency a USING (recipient_uei)
    LEFT JOIN top_state p USING (recipient_uei)
    ORDER BY obligations DESC
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--out-dir", default="aggregates")
    ap.add_argument("--src", help="override the parquet glob (e.g. a local path, for testing)")
    ap.add_argument("--memory-limit", default="8GB")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"SET temp_directory='{os.path.abspath(args.out_dir)}/tmp'")
    if args.src:
        src = args.src
    else:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            CREATE SECRET r2 (TYPE s3, PROVIDER credential_chain,
                ENDPOINT '{os.environ["AWS_ENDPOINT_URL_S3"].removeprefix("https://")}',
                REGION 'auto', URL_STYLE 'path')
        """)
        src = f"s3://{args.bucket}/{args.prefix}/contracts/FY*.parquet"
    os.makedirs(args.out_dir, exist_ok=True)

    for name, sql in [("recipient_year", RECIPIENT_YEAR), ("recipients", RECIPIENTS)]:
        out = os.path.join(args.out_dir, f"{name}.parquet")
        con.execute(f"COPY ({sql.format(src=src)}) TO '{out}' (FORMAT parquet, COMPRESSION zstd)")
        n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
        print(f"{out}: {n:,} rows, {os.path.getsize(out)/2**20:.0f} MB")


if __name__ == "__main__":
    main()
