# neon_olap/measurements/beetles.py

import pandas as pd
import numpy as np

def _prep_beetle(x):

    x = x.copy()

    x = x[x["sampleType"].str.lower() == "carabid"]
    x["site_location"] = x["plotID"]

    return x

def _expand_interval(df, measurement_id):

    rows = []

    for _, r in df.iterrows():
        dates = pd.date_range(r["setDate"], r["collectDate"] - pd.Timedelta(days=1), freq="D")

        for d in dates:
            rows.append({
                "date": d.date(),
                "site_location": r["site_location"],
                "value": r["value"]
            })

    out = pd.DataFrame(rows)
    out["measurement_id"] = measurement_id

    return out

def _beetle_diversity_components(x):

    x = _prep_beetle(x)

    species_counts = (
        x.groupby(["setDate", "collectDate", "site_location", "taxonID"], as_index=False)
         .agg(n=("individualCount", "sum"))
    )

    totals = (
        species_counts.groupby(["setDate", "collectDate", "site_location"], as_index=False)
        .agg(N=("n", "sum"))
    )

    df = species_counts.merge(totals, on=["setDate", "collectDate", "site_location"])
    df["p"] = df["n"] / df["N"]
    df["plogp"] = df["p"] * np.log(df["p"])

    H = (
        df.groupby(["setDate", "collectDate", "site_location"], as_index=False)
          .agg(H=("plogp", lambda s: -s.sum()))
    )

    S = (
        species_counts.groupby(["setDate", "collectDate", "site_location"], as_index=False)
        .agg(S=("taxonID", "nunique"))
    )

    return H.merge(S, on=["setDate", "collectDate", "site_location"])

def abundance_ground_beetle(x):

    x = _prep_beetle(x)

    df = (
        x.groupby(["setDate", "collectDate", "site_location"], as_index=False)
         .agg(value=("individualCount", "sum"))
    )

    return _expand_interval(df, "abundance_ground_beetle")

def species_richness_ground_beetle(x):

    x = _prep_beetle(x)

    df = (
        x.groupby(["setDate", "collectDate", "site_location"], as_index=False)
         .agg(value=("taxonID", "nunique"))
    )

    return _expand_interval(df, "species_richness_ground_beetle")

def shannon_diversity_ground_beetle(x):

    df = _beetle_diversity_components(x)
    df["value"] = df["H"]
    df.loc[np.isclose(df["value"], 0), "value"] = 0

    return _expand_interval(df, "shannon_diversity_ground_beetle")

def pielous_evenness_ground_beetle(x):

    df = _beetle_diversity_components(x)

    df["value"] = df["H"] / np.log(df["S"])
    df = df.dropna(subset=["value"])

    return _expand_interval(df, "pielous_evenness_ground_beetle")