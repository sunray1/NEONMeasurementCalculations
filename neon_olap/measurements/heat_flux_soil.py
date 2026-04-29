# neon_olap/measurements/heat_flux_soil.py

import pandas as pd

# ---------- heat_flux_soil ----------

def heat_flux_soil_mean(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("SHFMean", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "heat_flux_soil_mean"

    return out


def heat_flux_soil_maximum(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("SHFMaximum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "heat_flux_soil_maximum"

    return out


def heat_flux_soil_minimum(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("SHFMinimum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "heat_flux_soil_minimum"

    return out


def heat_flux_soil_temporal_variance(x):

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("SHFVariance", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "heat_flux_soil_temporal_variance"

    return out