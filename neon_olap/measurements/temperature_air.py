# neon_olap/measurements/temperature_air.py

import pandas as pd
import numpy as np

# ---------- temperature_air ----------

def temperature_air_mean(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("tempSingleMean", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_air_mean"

    return out


def temperature_air_maximum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("tempSingleMaximum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_air_maximum"

    return out


def temperature_air_minimum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("tempSingleMinimum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_air_minimum"

    return out


def temperature_air_temporal_variance(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("tempSingleVariance", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_air_temporal_variance"

    return out


def temperature_air_elevational_variance(x):

    # ---------- step 1: variance across vertical sensors ----------
    tmp = (
        x.groupby(
            ["startDateTime", "site_location"],
            as_index=False
        )
        .agg(value=("tempSingleMean", "var"))
    )

    tmp["date"] = pd.to_datetime(tmp["startDateTime"]).dt.date

    # ---------- step 2: daily mean ----------
    out = (
        tmp.groupby(["date", "site_location"], as_index=False)
           .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_air_elevational_variance"

    return out