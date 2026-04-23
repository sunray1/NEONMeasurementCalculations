# jobs/build_aggregates.py

import duckdb
from pathlib import Path

DB_PATH = Path("olap.duckdb")


def main():
    con = duckdb.connect(str(DB_PATH))

    print("Building aggregates...")

    # Example 1: site × month averages
    con.execute("""
        CREATE OR REPLACE TABLE agg_site_month AS
        SELECT
            measurement_id,
            site_id,
            year,
            month,
            AVG(value) AS avg_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            COUNT(*) AS n
        FROM fact_measurement
        GROUP BY 1,2,3,4
    """)

    print("  → agg_site_month")

    # Example 2: site × year averages
    con.execute("""
        CREATE OR REPLACE TABLE agg_site_year AS
        SELECT
            measurement_id,
            site_id,
            year,
            AVG(value) AS avg_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            COUNT(*) AS n
        FROM fact_measurement
        GROUP BY 1,2,3
    """)

    print("  → agg_site_year")

    # Example 3: overall by month (across sites)
    con.execute("""
        CREATE OR REPLACE TABLE agg_global_month AS
        SELECT
            measurement_id,
            year,
            month,
            AVG(value) AS avg_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            COUNT(*) AS n
        FROM fact_measurement
        GROUP BY 1,2,3
    """)

    print("  → agg_global_month")

    con.close()

    print("Done.")


if __name__ == "__main__":
    main()