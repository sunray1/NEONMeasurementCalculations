# neon_olap/measurements/co2_concentration_soil.py

import pandas as pd

# ---------- carbon_dioxide_concentration_soil ----------

def carbon_dioxide_concentration_soil_mean(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(
        value=("soilCO2concentrationMean", "mean")
    )
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "carbon_dioxide_concentration_soil_mean"
    return out


def carbon_dioxide_concentration_soil_maximum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(
        value=("soilCO2concentrationMaximum", "mean")
    )
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "carbon_dioxide_concentration_soil_maximum"
    return out


def carbon_dioxide_concentration_soil_minimum(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(
        value=("soilCO2concentrationMinimum", "mean")
    )
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "carbon_dioxide_concentration_soil_minimum"
    return out


def carbon_dioxide_concentration_soil_temporal_variance(x):
    out = x.groupby(["date", "site_location"], as_index=False).agg(
        value=("soilCO2concentrationVariance", "mean")
    )
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "carbon_dioxide_concentration_soil_temporal_variance"
    return out


def carbon_dioxide_concentration_soil_elevational_variance(x):

    # step 1: variance across depths within each horizontal sensor
    tmp = (
        x.groupby(
            ["startDateTime", "site_location", "sensor_horizontal"],
            as_index=False
        )
        .agg(value=("soilCO2concentrationMean", "var"))
    )

    # step 2: average across horizontal sensors
    tmp2 = (
        tmp.groupby(
            ["startDateTime", "site_location"],
            as_index=False
        )
        .agg(value=("value", "mean"))
    )

    tmp2["date"] = pd.to_datetime(tmp2["startDateTime"]).dt.date

    # step 3: daily mean
    out = (
        tmp2.groupby(["date", "site_location"], as_index=False)
            .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "carbon_dioxide_concentration_soil_elevational_variance"

    return out