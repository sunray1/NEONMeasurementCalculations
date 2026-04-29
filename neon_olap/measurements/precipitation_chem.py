# neon_olap/measurements/precipitation_chem.py

import pandas as pd

def _expand_interval(x, value_col, measurement_id):
    rows = []

    for _, r in x.iterrows():
        if pd.isna(r["setDate"]) or pd.isna(r["collectDate"]):
            continue

        if pd.isna(r[value_col]):
            continue

        dates = pd.date_range(r["setDate"], r["collectDate"] - pd.Timedelta(days=1), freq="D")

        for d in dates:
            rows.append({
                "date": d.date(),
                "site_location": r["site_location"],
                "value": r[value_col]
            })

    out = pd.DataFrame(rows)
    out["measurement_id"] = measurement_id

    return out

def ammonium_concentration_precipitation(x):
    return _expand_interval(x, "precipAmmonium", "ammonium_concentration_precipitation")


def bromide_concentration_precipitation(x):
    return _expand_interval(x, "precipBromide", "bromide_concentration_precipitation")


def calcium_concentration_precipitation(x):
    return _expand_interval(x, "precipCalcium", "calcium_concentration_precipitation")


def chloride_concentration_precipitation(x):
    return _expand_interval(x, "precipChloride", "chloride_concentration_precipitation")


def conductivity_precipitation(x):
    return _expand_interval(x, "precipConductivity", "conductivity_precipitation")


def magnesium_concentration_precipitation(x):
    return _expand_interval(x, "precipMagnesium", "magnesium_concentration_precipitation")


def nitrate_concentration_precipitation(x):
    return _expand_interval(x, "precipNitrate", "nitrate_concentration_precipitation")


def ph_precipitation(x):
    return _expand_interval(x, "pH", "ph_precipitation")


def phosphate_concentration_precipitation(x):
    return _expand_interval(x, "precipPhosphate", "phosphate_concentration_precipitation")


def potassium_concentration_precipitation(x):
    return _expand_interval(x, "precipPotassium", "potassium_concentration_precipitation")


def sodium_concentration_precipitation(x):
    return _expand_interval(x, "precipSodium", "sodium_concentration_precipitation")


def sulfate_concentration_precipitation(x):
    return _expand_interval(x, "precipSulfate", "sulfate_concentration_precipitation")