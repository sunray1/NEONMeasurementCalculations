# neon_olap/measurements/land_birds.py

import pandas as pd
import numpy as np

def _prep_bird(x):

    # ---------- clean ----------
    x = x[x["taxonID"].notna()].copy()
    x["clusterSize"] = x["clusterSize"].fillna(1)

    # ---------- location ----------
    x["site_location"] = x["plotID"]
    
    # ---------- date ----------
    x["startDate"] = pd.to_datetime(x["startDate"]).dt.date

    return x

def _bird_diversity_components(x):

    x = _prep_bird(x)

    species_counts = (
        x.groupby(["startDate", "site_location", "taxonID"], as_index=False)
         .agg(n=("clusterSize", "sum"))
    )

    totals = (
        species_counts.groupby(["startDate", "site_location"], as_index=False)
        .agg(N=("n", "sum"))
    )

    df = species_counts.merge(totals, on=["startDate", "site_location"])
    df["p"] = df["n"] / df["N"]
    df["plogp"] = df["p"] * np.log(df["p"])

    H = (
        df.groupby(["startDate", "site_location"], as_index=False)
          .agg(H=("plogp", lambda s: -s.sum()))
    )

    S = (
        species_counts.groupby(["startDate", "site_location"], as_index=False)
        .agg(S=("taxonID", "nunique"))
    )

    return H.merge(S, on=["startDate", "site_location"])


def abundance_bird(x):

    x = _prep_bird(x)

    out = (
        x.groupby(["startDate", "site_location"], as_index=False)
         .agg(value=("clusterSize", "sum"))
    )

    out["measurement_id"] = "abundance_bird"

    return out.rename(columns={"startDate": "date"})


def species_richness_bird(x):

    x = _prep_bird(x)

    out = (
        x.groupby(["startDate", "site_location"], as_index=False)
         .agg(value=("taxonID", "nunique"))
    )

    out["measurement_id"] = "species_richness_bird"

    return out.rename(columns={"startDate": "date"})


def shannon_diversity_bird(x):

    df = _bird_diversity_components(x)

    df["value"] = df["H"]
    df["measurement_id"] = "shannon_diversity_bird"

    return df.rename(columns={"startDate": "date"})[
        ["date", "site_location", "measurement_id", "value"]
    ]


def pielous_evenness_bird(x):

    df = _bird_diversity_components(x)

    df["value"] = df["H"] / np.log(df["S"])
    df.loc[df["S"] <= 1, "value"] = np.nan
    df["measurement_id"] = "pielous_evenness_bird"

    return df.rename(columns={"startDate": "date"})[
        ["date", "site_location", "measurement_id", "value"]
    ]