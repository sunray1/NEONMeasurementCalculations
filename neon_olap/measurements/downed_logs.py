# neon_olap/measurements/downed_logs.py

import pandas as pd
import numpy as np

def _prep_downed_log(x):

    x = x.copy()
    
    # filter
    x = x[x["targetTaxaPresent"] == "Y"]

    # location
    x["site_location"] = x["plotID"]

    # date
    x["date"] = pd.to_datetime(x["date"]).dt.date

    return x

def abundance_downed_log(x):

    x = _prep_downed_log(x)

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("plotID", "size"))
    )

    out["measurement_id"] = "abundance_downed_log"

    return out

def downed_log_estimated_volume(x):

    x = _prep_downed_log(x)

    x["volume"] = (
        np.pi
        * (x["equivalentLogDiameter"] / 200) ** 2
        * x["logLength"]
        / x["volumeFactor"]
    )

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("volume", "sum"))
    )

    out["measurement_id"] = "downed_log_estimated_volume"

    return out

def downed_log_mean_decay_class(x):

    x = _prep_downed_log(x)

    x = x[x["decayClass"] != "6 - NA"].copy()
    
    x["decayClass"] = (
        x["decayClass"]
        .str.extract(r"^(\d+)")   # grab leading number
        .astype(float)
    )

    out = (
        x.groupby(["date", "site_location"], as_index=False)
         .agg(value=("decayClass", "mean"))
    )

    out["measurement_id"] = "downed_log_mean_decay_class"

    return out