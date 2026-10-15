import json
from pathlib import Path
from typing import Iterator, Sequence
import pandas as pd

FIXTURES = Path(__file__).parent / "fixtures" / "energy_charts"


class FixtureEnergyChartsSource:
    """Reads recorded Energy-Charts public_power responses from fixtures."""

    def __init__(self, country: str = "de"):
        self.country = country
        self.payload = None # caching in init -  runs faster for large json
        self._df = None  

    def _files(self) -> list[Path]:        
        return sorted(FIXTURES.glob(f"public_power_{self.country}_*.json"))

    def _load(self) -> dict:
        """Merge all fixture files into one payload (unix_seconds + types)."""

        if self.payload is None:
            seconds, types = [],{}
            for f in self._files():                
                with f.open() as fp:
                    p = json.load(fp)
                    seconds.extend(p["unix_seconds"])
                    for t in p["production_types"]:
                        types.setdefault(t["name"], []).extend(t["data"])
            self.payload = {"unix_seconds":seconds,"production_types":types}
        return self.payload

    def types(self) -> list[str]:
        """list available types"""
        return list(self._load()["production_types"].keys())

    
    def load_df(self) -> pd.DataFrame:
        """Wide DataFrame: index = ts_utc, columns = production types.
        
        Values are power in MW (EC native), 15-minute resolution.
        """
        if self._df is None:
            self.payload = self._load()
            self._df = pd.DataFrame(
                self.payload["production_types"],
                index=pd.to_datetime(self.payload["unix_seconds"], unit="s", utc=True),
            )
            self._df.index.name = "ts_utc"
        return self._df

    
    def chunk(self, production_type: str) -> Iterator[tuple[int, float]]:
        """Yield (ts_ms, power_mw) for one production type, whole fixture range."""
        payload = self._load()
        data = payload["production_types"].get(production_type)
        if data is None:
            return
        for ts_s, mw in zip(payload["unix_seconds"], data):
            if mw is None:
                continue
            yield ts_s * 1000, mw            



EC_DDL = """
CREATE OR REPLACE TABLE bronze_energy_charts (
ts_utc          TIMESTAMPTZ NOT NULL,
country         VARCHAR     NOT NULL,
production_type VARCHAR     NOT NULL,
power_mw        DOUBLE,
PRIMARY KEY (ts_utc, country, production_type)
);
"""
