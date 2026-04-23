# neon_olap/measurements/air_temp_mean.py

import pandas as pd


class AirTempDailyMean:
    spec = {
        "measurement_id": "air_temp_daily_mean"
    }

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        x = df.copy()
        x["date"] = pd.to_datetime(x["datetime"]).dt.date

        out = (
            x.groupby(["site_id", "date"], as_index=False)
             .agg(value=("air_temp", "mean"))
        )

        out["measurement_id"] = self.spec["measurement_id"]

        return out