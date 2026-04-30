# neon_olap/measurements/water_content_salinity_soil.py

import pandas as pd

# ---------- ion_content ----------

def ion_content_salinity_soil_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSICMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "ion_content_salinity_soil_mean"
    return out


def ion_content_salinity_soil_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSICMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "ion_content_salinity_soil_maximum"
    return out


def ion_content_salinity_soil_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSICMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "ion_content_salinity_soil_minimum"
    return out


def ion_content_salinity_soil_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSICVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "ion_content_salinity_soil_temporal_variance"
    return out


def ion_content_salinity_soil_elevational_variance(x):

    # ---------- step 1: variance across depths within each horizontal ----------
    tmp = (
        x.groupby(
            ["startDateTime", "site_location", "sensor_horizontal"],
            as_index=False
        )
        .agg(value=("VSICMean", "var"))
    )

    # ---------- step 2: average across horizontals ----------
    tmp2 = (
        tmp.groupby(["startDateTime", "site_location"], as_index=False)
           .agg(value=("value", "mean"))
    )

    tmp2["date"] = pd.to_datetime(tmp2["startDateTime"]).dt.date

    # ---------- step 3: daily mean ----------
    out = (
        tmp2.groupby(["date", "site_location"], as_index=False)
            .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "ion_content_salinity_soil_elevational_variance"

    return out

# ---------- water_content ----------

def water_content_soil_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSWCMean", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "water_content_soil_mean"
    return out


def water_content_soil_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSWCMaximum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "water_content_soil_maximum"
    return out


def water_content_soil_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSWCMinimum", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "water_content_soil_minimum"
    return out


def water_content_soil_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(value=("VSWCVariance", "mean"))
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "water_content_soil_temporal_variance"
    return out


def water_content_soil_elevational_variance(x):

    # ---------- step 1: variance across depths within each horizontal ----------
    tmp = (
        x.groupby(
            ["startDateTime", "site_location", "sensor_horizontal"],
            as_index=False
        )
        .agg(value=("VSWCMean", "var"))
    )

    # ---------- step 2: average across horizontals ----------
    tmp2 = (
        tmp.groupby(["startDateTime", "site_location"], as_index=False)
           .agg(value=("value", "mean"))
    )

    tmp2["date"] = pd.to_datetime(tmp2["startDateTime"]).dt.date

    # ---------- step 3: daily mean ----------
    out = (
        tmp2.groupby(["date", "site_location"], as_index=False)
            .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "water_content_soil_elevational_variance"

    return out