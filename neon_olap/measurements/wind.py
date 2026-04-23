# neon_olap/measurements/wind.py

import pandas as pd
import numpy as np

# ---------- wind_direction ----------
# value is the direction the wind is COMING FROM
#0° (or 360°) = North
#90°           = East
#180°          = South
#270°          = West

def wind_direction_mean(x):

    # convert degrees → radians
    rad = np.deg2rad(x["windDirMean"])

    # compute sin/cos components
    x["sin"] = np.sin(rad)
    x["cos"] = np.cos(rad)

    # group and average components
    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(
             sin_mean=("sin", "mean"),
             cos_mean=("cos", "mean")
         )
    )

    # convert back to angle
    out["value"] = np.rad2deg(np.arctan2(out["sin_mean"], out["cos_mean"]))

    # normalize to [0, 360)
    out["value"] = (out["value"] + 360) % 360

    out = out.drop(columns=["sin_mean", "cos_mean"])
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_direction_mean"

    return out

def wind_direction_temporal_variance(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("windDirVariance", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_direction_temporal_variance"

    return out

def wind_direction_elevational_variance(x):

    theta = np.deg2rad(90 - x["windDirMean"])
    x["sin"] = np.sin(theta)
    x["cos"] = np.cos(theta)

    tmp = (
        x.groupby(["startDateTime", "site_location"], as_index=False)
         .agg(
             sin_mean=("sin", "mean"),
             cos_mean=("cos", "mean"),
             n_vertical=(
                 "sensor_vertical",
                 lambda s: s[x.loc[s.index, "windDirMean"].notna()].nunique()
             )
         )
    )

    R = np.sqrt(tmp["sin_mean"]**2 + tmp["cos_mean"]**2)

    # avoid log(0)
    R = np.clip(R, 1e-8, 1.0)

    # circular variance in radians²
    var_rad2 = -2 * np.log(R)

    # convert to degrees²
    tmp["value"] = np.rad2deg(np.sqrt(var_rad2))**2

    # undefined if only one vertical sensor
    tmp.loc[tmp["n_vertical"] < 2, "value"] = np.nan

    tmp["date"] = pd.to_datetime(tmp["startDateTime"]).dt.date
    from datetime import date

    out = (
        tmp.groupby(["date", "site_location"], as_index=False)
           .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_direction_elevational_variance"

    return out

# ---------- wind_speed ----------

def wind_speed_mean(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("windSpeedMean", "mean"))
    )
    
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_speed_mean"
    
    return out


def wind_speed_maximum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("windSpeedMaximum", "mean"))
    )
    
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_speed_maximum"
    
    return out


def wind_speed_minimum(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("windSpeedMinimum", "mean"))
    )
    
    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_speed_minimum"
    
    return out


def wind_speed_temporal_variance(x):

    out = (
        x.groupby(
            ["date", "site_location"],
            as_index=False
        )
        .agg(value=("windSpeedVariance", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_speed_temporal_variance"

    return out

def wind_speed_elevational_variance(x):

    # ---------- step 1: variance across vertical sensors ----------
    tmp = (
        x.groupby(
            ["startDateTime", "site_location"],
            as_index=False
        )
        .agg(value=("windSpeedMean", "var"))
    )

    tmp["date"] = pd.to_datetime(tmp["startDateTime"]).dt.date

    # ---------- step 2: daily mean ----------
    out = (
        tmp.groupby(["date", "site_location"], as_index=False)
           .agg(value=("value", "mean"))
    )

    out = out.dropna(subset=["value"])
    out["measurement_id"] = "wind_speed_elevational_variance"

    return out