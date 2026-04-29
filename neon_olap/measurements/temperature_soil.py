# neon_olap/measurements/temperature_soil.py

import pandas as pd

# ---------- temperature_soil ----------

def temperature_soil_mean(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("soilTempMean", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_soil_mean"

    return out


def temperature_soil_maximum(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("soilTempMaximum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_soil_maximum"

    return out


def temperature_soil_minimum(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("soilTempMinimum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_soil_minimum"

    return out


def temperature_soil_temporal_variance(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("soilTempVariance", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_soil_temporal_variance"

    return out


def temperature_soil_elevational_variance(x):

    # ---------- step 1: variance across depths within each horizontal ----------
    tmp = (
        x.groupby(
            ["startDateTime", "site_location", "sensor_horizontal"],
            as_index=False
        )
        .agg(value=("soilTempMean", "var"))
    )

    # ---------- step 2: average across horizontals ----------
    tmp2 = (
        tmp.groupby(
            ["startDateTime", "site_location"],
            as_index=False
        )
        .agg(value=("value", "mean"))
    )

    tmp2["date"] = pd.to_datetime(tmp2["startDateTime"]).dt.date

    # ---------- step 3: daily mean ----------
    out = (
        tmp2.groupby(["date", "site_location"], as_index=False)
            .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_soil_elevational_variance"

    return out