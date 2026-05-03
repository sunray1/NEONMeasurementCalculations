# neon_olap/measurements/land_birds.py

import pandas as pd
import numpy as np

def _prep_bird(x):

    # ---------- clean ----------
    x = x[x["taxonID"].notna()].copy()
    x["clusterSize"] = x["clusterSize"].fillna(1)

    # ---------- location ----------
    x["site_location"] = x["plotID"]

    # ---------- year ----------
    x["year"] = pd.to_datetime(x["startDate"]).dt.year

    return x

def _expand_yearly_to_daily(yearly, measurement_id):

    rows = []

    for _, r in yearly.iterrows():
        dates = pd.date_range(
            f"{r['year']}-01-01",
            f"{r['year']}-12-31",
            freq="D"
        )

        for d in dates:
            rows.append({
                "date": d.date(),
                "site_location": r["site_location"],
                "value": r["value"]
            })

    out = pd.DataFrame(rows)
    out["measurement_id"] = measurement_id

    return out

def _bird_diversity_components(x):

    x = _prep_bird(x)

    species_counts = (
        x.groupby(["year", "site_location", "taxonID"], as_index=False)
         .agg(n=("clusterSize", "sum"))
    )

    totals = (
        species_counts.groupby(["year", "site_location"], as_index=False)
        .agg(N=("n", "sum"))
    )

    df = species_counts.merge(totals, on=["year", "site_location"])
    df["p"] = df["n"] / df["N"]
    df["plogp"] = df["p"] * np.log(df["p"])

    H = (
        df.groupby(["year", "site_location"], as_index=False)
          .agg(H=("plogp", lambda s: -s.sum()))
    )

    S = (
        species_counts.groupby(["year", "site_location"], as_index=False)
        .agg(S=("taxonID", "nunique"))
    )

    return H.merge(S, on=["year", "site_location"])

def abundance_bird(x):

    x = _prep_bird(x)

    yearly = (
        x.groupby(["year", "site_location"], as_index=False)
         .agg(value=("clusterSize", "sum"))
    )

    return _expand_yearly_to_daily(
        yearly,
        "abundance_bird"
    )

def species_richness_bird(x):

    x = _prep_bird(x)

    yearly = (
        x.groupby(["year", "site_location"], as_index=False)
         .agg(value=("taxonID", "nunique"))
    )

    return _expand_yearly_to_daily(
        yearly,
        "species_richness_bird"
    )

def shannon_diversity_bird(x):

    df = _bird_diversity_components(x)

    df["value"] = df["H"]

    return _expand_yearly_to_daily(
        df[["year", "site_location", "value"]],
        "shannon_diversity_bird"
    )

def pielous_evenness_bird(x):

    df = _bird_diversity_components(x)

    df["value"] = df["H"] / np.log(df["S"])
    df.loc[df["S"] <= 1, "value"] = np.nan

    return _expand_yearly_to_daily(
        df[["year", "site_location", "value"]],
        "pielous_evenness_bird"
    )

