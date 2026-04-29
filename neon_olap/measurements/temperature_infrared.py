# neon_olap/measurements/temperature_infrared.py

import pandas as pd
import numpy as np

# ---------- temperature_infrared ----------

def temperature_infrared_mean(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("bioTempMean", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_infrared_mean"

    return out


def temperature_infrared_maximum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("bioTempMaximum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_infrared_maximum"

    return out


def temperature_infrared_minimum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("bioTempMinimum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_infrared_minimum"

    return out


def temperature_infrared_temporal_variance(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("bioTempVariance", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_infrared_temporal_variance"

    return out


def temperature_infrared_elevational_variance(x):

    # ---------- restrict to tower ----------
    x = x[x["site_location"].str.contains("tower")]

    # ---------- step 1: variance across vertical sensors ----------
    tmp = (
        x.groupby(
            ["startDateTime", "site_location"],
            as_index=False
        )
        .agg(value=("bioTempMean", "var"))
    )

    tmp["date"] = pd.to_datetime(tmp["startDateTime"]).dt.date

    # ---------- step 2: daily mean ----------
    out = (
        tmp.groupby(["date", "site_location"], as_index=False)
           .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_infrared_elevational_variance"

    return out