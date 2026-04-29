# neon_olap/measurements/radiation.py

import pandas as pd

def _get_column(x, candidates):
    for col in candidates:
        if col in x.columns:
            return col

# ---------- PAR incoming ----------

def photosynthetically_active_radiation_incoming_mean(x):
    col = _get_column(x, ["PARMean", "linePARMean"])
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=(col, "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_incoming_mean"
    return out


def photosynthetically_active_radiation_incoming_maximum(x):
    col = _get_column(x, ["PARMaximum", "linePARMaximum"])
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=(col, "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_incoming_maximum"
    return out


def photosynthetically_active_radiation_incoming_minimum(x):
    col = _get_column(x, ["PARMinimum", "linePARMinimum"])
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=(col, "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_incoming_minimum"
    return out


def photosynthetically_active_radiation_incoming_temporal_variance(x):
    col = _get_column(x, ["PARVariance", "linePARVariance"])
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=(col, "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_incoming_temporal_variance"
    return out

def photosynthetically_active_radiation_incoming_elevational_variance(x):
    tmp = (
        x.groupby(["startDateTime", "site_location"], as_index=False)
         .agg(value=("PARMean", "var"))
    )
    tmp["date"] = pd.to_datetime(tmp["startDateTime"]).dt.date

    out = (
        tmp.groupby(["date", "site_location"], as_index=False)
           .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_incoming_elevational_variance"
    return out


# ---------- PAR outgoing ----------

def photosynthetically_active_radiation_outgoing_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outPARMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_outgoing_mean"
    return out


def photosynthetically_active_radiation_outgoing_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outPARMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_outgoing_maximum"
    return out


def photosynthetically_active_radiation_outgoing_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outPARMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_outgoing_minimum"
    return out


def photosynthetically_active_radiation_outgoing_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("outPARVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "photosynthetically_active_radiation_outgoing_temporal_variance"
    return out