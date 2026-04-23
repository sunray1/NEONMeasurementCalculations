# jobs/run.py

import argparse
import pandas as pd

from neon_olap.api import discover_urls
from neon_olap.adapters.adapter import Adapter
from neon_olap.measurements import MEASUREMENTS
from neon_olap.db import get_connection, init_db, insert_facts, insert_dates, insert_locations, insert_measurements
from neon_olap.config import SITES, YEARS, MONTHS

adapter = Adapter()

def parse_args():
    p = argparse.ArgumentParser(
        description="Run NEON OLAP pipeline over configured sites/years/months. "
                    "Optionally filter by product or measurement."
    )

    # filters
    p.add_argument(
        "--dataproduct",
        help="NEON data product ID to run (e.g. DP1.00002.001). "
             "If not provided, runs all products in tasks.csv."
    )

    p.add_argument(
        "--measurement",
        help="Measurement variable name to run (e.g. wind_speed_mean). "
             "If not provided, runs all measurements for each product."
    )

    # execution overrides
    p.add_argument(
        "--site",
        help="Site code to run (e.g. HARV, ABBY). "
             "Overrides SITES in config if provided."
    )

    p.add_argument(
        "--year",
        type=int,
        help="Year to run (e.g. 2021). Overrides YEARS in config if provided."
    )

    p.add_argument(
        "--month",
        type=int,
        help="Month to run (1-12). Overrides MONTHS in config if provided."
    )

    return p.parse_args()


def run_job(con, dataproduct, site, year, months, measurement_names):
    #defined as a dataproduct x site x year-month
    
    print(f"{dataproduct} | {site} | {year}")

    monthly_dfs = []

    # ---------- collect raw data across months ----------
    for month in months:
        print(f"  loading {year}-{month}")

        urls = discover_urls(dataproduct, site, year, month)
        tables = adapter.load(dataproduct, urls, site)
        df = tables["main"]

        monthly_dfs.append(df)

    if not monthly_dfs:
        return

    # combine all months
    full_df = pd.concat(monthly_dfs, ignore_index=True)
    from datetime import date

    # ---------- run measurements ----------
    all_facts = []

    for name in measurement_names:
        print(f"  → {name}")

        func = MEASUREMENTS[name]
        fact_df = func(full_df)

        all_facts.append(fact_df)
    if not all_facts:
        return

    combined_df = pd.concat(all_facts, ignore_index=True)

    # ---------- single insert ----------
    insert_dates(con, combined_df)
    insert_locations(con, combined_df, site)
    insert_measurements(con, combined_df, dataproduct)
    insert_facts(con, combined_df)

def main():
    args = parse_args()

    tasks = pd.read_csv(f"metadata/measurements.csv", encoding="latin1")

    # apply filters
    if args.dataproduct:
        tasks = tasks[tasks["dataProduct"] == args.dataproduct]

    if args.measurement:
        tasks = tasks[tasks["measurementID"] == args.measurement]

    grouped = tasks.groupby("dataProduct")

    sites = [args.site] if args.site else SITES
    years = [args.year] if args.year else YEARS
    months = [args.month] if args.month else MONTHS

    con = get_connection()
    init_db(con)
    
    for dataproduct, group in grouped:
        measurement_names = group["measurementID"].tolist()
    
        for site in sites:
            for year in years:
                run_job(
                    con=con,
                    dataproduct=dataproduct,
                    site=site,
                    year=year,
                    months=months,
                    measurement_names=measurement_names,
                )
    
    con.close()

if __name__ == "__main__":
    main()