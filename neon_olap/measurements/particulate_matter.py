# neon_olap/measurements/particulate_matter.py

import pandas as pd

# ---------- particulate_matter ----------

def particulate_matter_le_1_um_concentration(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("PM1Median", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "particulate_matter_le_1_um_concentration"

    return out


def particulate_matter_le_10_um_concentration(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("PM10Median", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "particulate_matter_le_10_um_concentration"

    return out


def particulate_matter_le_15_um_concentration(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("PM15Median", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "particulate_matter_le_15_um_concentration"

    return out


def particulate_matter_le_25_um_concentration(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("PM2.5Median", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "particulate_matter_le_25_um_concentration"

    return out


def particulate_matter_le_4_um_concentration(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("PM4Median", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "particulate_matter_le_4_um_concentration"

    return out