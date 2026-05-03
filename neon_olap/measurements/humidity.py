# neon_olap/measurements/humidity.py

import pandas as pd

# ---------- humidity ----------

def humidity_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("RHMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "humidity_mean"
    return out


def humidity_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("RHMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "humidity_maximum"
    return out


def humidity_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("RHMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "humidity_minimum"
    return out


def humidity_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("RHVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "humidity_temporal_variance"
    return out

# ---------- dew_or_frost_point ----------

def temperature_dew_or_frost_point_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("dewTempMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_dew_or_frost_point_mean"
    return out


def temperature_dew_or_frost_point_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("dewTempMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_dew_or_frost_point_maximum"
    return out


def temperature_dew_or_frost_point_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("dewTempMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_dew_or_frost_point_minimum"
    return out


def temperature_dew_or_frost_point_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("dewTempVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "temperature_dew_or_frost_point_temporal_variance"
    return out