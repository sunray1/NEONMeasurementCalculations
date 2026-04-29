# neon_olap/adapters/adapter.py

import json
import duckdb
from pathlib import Path
import pandas as pd


class Adapter:

    def __init__(self, config_path="../../metadata/adapters.json", site_map_path="../../metadata/site_metadata.csv"):
        base_dir = Path(__file__).parent

        # load adapter config
        with open(base_dir / config_path) as f:
            self.config = json.load(f)

        # load site → site_type mapping
        site_df = pd.read_csv(base_dir / site_map_path)
        self.site_map = dict(zip(site_df["site_id"], site_df["site_type"]))


    def load(self, product, urls, site):
        cfg = self.config[product]
    
        # ---------- config ----------
        tables = cfg["tables"]
        has_sensors = cfg.get("has_sensors", False)
        fields = cfg.get("fields")
    
        # handle fields as dict (col → type) or list
        if isinstance(fields, dict):
            field_names = list(fields.keys())
        else:
            field_names = fields
    
        field_sql = ", ".join(f'"{f}"' for f in field_names) if field_names else "*"
    
        # ---------- select URLs ----------
        selected = [
            u for u in urls
            if any(t in u for t in tables)
        ]
    
        if not selected:
            return {"main": pd.DataFrame()}
    
        con = duckdb.connect()
    
        # ---------- load data ----------
        if has_sensors:
            dfs = []
    
            for url in selected:
                horiz, vert = self._extract_sensor(url)
    
                df = con.execute(
                    f"""
                    SELECT {field_sql},
                           ? AS sensor_horizontal,
                           ? AS sensor_vertical
                    FROM read_csv_auto(?)
                    """,
                    [horiz, vert, url],
                ).df()
                
        # ---------- sensor location mapping ----------
                sensor_map = cfg.get("sensor_location_mapping")
                if sensor_map:
                    sensor_key = f"{horiz}.{vert}"
                    location_type = sensor_map.get(sensor_key)
                    df["site_location"] = f"{site}_{location_type}"
                    
    
                dfs.append(df)
    
            combined = pd.concat(dfs, ignore_index=True)
    
        else:
            combined = con.execute(
                f"SELECT {field_sql} FROM read_csv_auto(?)",
                [selected],
            ).df()

        # ---------- enforce types ----------
        if isinstance(fields, dict):
            for col, dtype in fields.items():
                if col not in combined.columns:
                    continue
    
                if dtype == "float":
                    combined[col] = pd.to_numeric(combined[col], errors="coerce")
    
                elif dtype == "datetime":
                    combined[col] = pd.to_datetime(combined[col], utc=True, errors="coerce")
    
        # ---------- add date ----------
        if "startDateTime" in combined.columns:
            combined["date"] = combined["startDateTime"].dt.date
    
        # ---------- resolve location ----------
        site_type = self.site_map.get(site)
    
        location_mapping = cfg.get("location_mapping", {})
        location_type = location_mapping.get(site_type)
    
        if location_mapping and location_type is None:
            raise ValueError(
                f"No location mapping for site_type={site_type} in {product}"
            )
    
        if location_type:
            combined["site_location"] = f"{site}_{location_type}"
    
        return {"main": combined}


    def _extract_sensor(self, url):
        """
        Extract horizontal and vertical sensor positions.

        Example:
        NEON.D01.HARV.DP1.00001.001.000.020.030.2DWSD_30min
        NEON.D10.ARIK.DP1.00001.001.200.000.002.2DWSD_2min
                                     ↑   ↑
                                   horiz vert
        """

        name = url.split("/")[-1]
        parts = name.split(".")

        horizontal = parts[6]
        vertical = parts[7]

        return horizontal, vertical