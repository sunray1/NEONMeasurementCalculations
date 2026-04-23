# neon_olap/db.py

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("outputs/full/olap.duckdb")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_connection():
    return duckdb.connect(str(DB_PATH))


def init_db(con):
    # ---------- dimensions ----------
    con.execute("""
        CREATE TABLE IF NOT EXISTS date_dimension (
            dateID DATE PRIMARY KEY,
            doy INTEGER,
            year INTEGER,
            season TEXT,
            month INTEGER,
            week_of_year INTEGER,
            day INTEGER
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS location_dimension (
            locationID TEXT PRIMARY KEY,
            site TEXT,
            state TEXT,
            domain TEXT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS measurement_dimension (
            measurementID TEXT PRIMARY KEY,
            measurementName TEXT,
            units TEXT,
            dataProduct TEXT
        )
    """)

    # ---------- fact ----------
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_measurement (
            measurementID TEXT,
            locationID TEXT,
            dateID DATE,
            value DOUBLE,
            PRIMARY KEY (measurementID, locationID, dateID)
        )
    """)
    
def insert_dates(con, df: pd.DataFrame):
    date_df = df[["date"]].drop_duplicates().copy()

    # ensure datetime
    date_df["date"] = pd.to_datetime(date_df["date"])

    # ---------- derive fields ----------
    date_df["year"] = date_df["date"].dt.year
    date_df["month"] = date_df["date"].dt.month
    date_df["day"] = date_df["date"].dt.day
    date_df["doy"] = date_df["date"].dt.dayofyear
    date_df["week_of_year"] = date_df["date"].dt.isocalendar().week.astype(int)

    # season (simple meteorological seasons)
    def _season(month):
        if month in [12, 1, 2]:
            return "winter"
        elif month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        else:
            return "fall"

    date_df["season"] = date_df["month"].apply(_season)

    con.register("date_df", date_df)

    con.execute("""
        INSERT OR IGNORE INTO date_dimension
        SELECT
            date AS dateID,
            doy,
            year,
            season,
            month,
            week_of_year,
            day
        FROM date_df
    """)
    
def insert_locations(con, df: pd.DataFrame, site: str):
    # unique locations (still need site_location for ID)
    loc_df = df[["site_location"]].drop_duplicates().copy()

    # use site passed in (not derived)
    loc_df["site"] = site

    # load metadata
    site_meta = pd.read_csv("metadata/site_metadata.csv")

    # join to get state + domain
    loc_df = loc_df.merge(site_meta, left_on="site", right_on="site_id", how="left")

    # locationID is the full location string
    loc_df["locationID"] = loc_df["site_location"]

    loc_df = loc_df[[
        "locationID",
        "site_id",
        "site_state",
        "domain_id"
    ]]

    con.register("loc_df", loc_df)

    con.execute("""
        INSERT OR IGNORE INTO location_dimension
        SELECT locationID, site_id, site_state, domain_id
        FROM loc_df
    """)
    
def insert_measurements(con, df: pd.DataFrame, product: str):
    # unique measurements from this batch
    meas_df = df[["measurement_id"]].drop_duplicates().copy()

    # load metadata
    meta = pd.read_csv("metadata/measurements.csv", encoding="latin1")

    # join metadata
    meas_df = meas_df.merge(meta, left_on="measurement_id", right_on="measurementID", how="left")

    # final columns
    meas_df["measurementID"] = meas_df["measurementID"]
    meas_df["measurementName"] = meas_df["measurement_name"]
    meas_df["dataProduct"] = product

    meas_df = meas_df[[
        "measurementID",
        "measurementName",
        "units",
        "dataProduct"
    ]]

    con.register("meas_df", meas_df)

    con.execute("""
        INSERT OR IGNORE INTO measurement_dimension
        SELECT measurementID, measurementName, units, dataProduct
        FROM meas_df
    """)

def insert_facts(con, df: pd.DataFrame):
    fact_df = df.copy()

    fact_df["locationID"] = fact_df["site_location"]
    fact_df["measurementID"] = fact_df["measurement_id"]
    fact_df["dateID"] = fact_df["date"]

    fact_df = fact_df[[
        "measurementID",
        "locationID",
        "dateID",
        "value"
    ]]

    con.register("fact_df", fact_df)

    con.execute("""
        INSERT INTO fact_measurement
        SELECT * FROM fact_df
        ON CONFLICT (measurementID, locationID, dateID) DO UPDATE SET
            value = excluded.value
    """)
