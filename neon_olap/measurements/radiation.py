# neon_olap/measurements/radiation.py

import pandas as pd

# ---------- longwave incoming ----------

def longwave_radiation_incoming_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inLWMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_incoming_mean"
    return out


def longwave_radiation_incoming_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inLWMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_incoming_maximum"
    return out


def longwave_radiation_incoming_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inLWMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_incoming_minimum"
    return out


def longwave_radiation_incoming_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inLWVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_incoming_temporal_variance"
    return out


# ---------- longwave outgoing ----------

def longwave_radiation_outgoing_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outLWMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_outgoing_mean"
    return out


def longwave_radiation_outgoing_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outLWMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_outgoing_maximum"
    return out


def longwave_radiation_outgoing_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outLWMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_outgoing_minimum"
    return out


def longwave_radiation_outgoing_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outLWVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "longwave_radiation_outgoing_temporal_variance"
    return out


# ---------- shortwave incoming ----------

def shortwave_radiation_incoming_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inSWMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_incoming_mean"
    return out


def shortwave_radiation_incoming_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inSWMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_incoming_maximum"
    return out


def shortwave_radiation_incoming_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inSWMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_incoming_minimum"
    return out


def shortwave_radiation_incoming_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("inSWVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_incoming_temporal_variance"
    return out


# ---------- shortwave outgoing ----------

def shortwave_radiation_outgoing_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outSWMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_outgoing_mean"
    return out


def shortwave_radiation_outgoing_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outSWMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_outgoing_maximum"
    return out


def shortwave_radiation_outgoing_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outSWMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_outgoing_minimum"
    return out


def shortwave_radiation_outgoing_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outSWVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "shortwave_radiation_outgoing_temporal_variance"
    return out