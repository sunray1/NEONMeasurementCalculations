# neon_olap/measurements/barometric_pressure.py

import pandas as pd
import numpy as np

# ---------- barometric_pressure ----------

def barometric_pressure_mean(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("staPresMean", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "barometric_pressure_mean"

    return out


def barometric_pressure_maximum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("staPresMaximum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "barometric_pressure_maximum"

    return out


def barometric_pressure_minimum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("staPresMinimum", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "barometric_pressure_minimum"

    return out


def barometric_pressure_temporal_variance(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("staPresVariance", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "barometric_pressure_temporal_variance"

    return out