from datetime import date, datetime, timezone
from typing import Sequence
import duckdb
from utils import settings

from data_platform.energy_charts import FixtureEnergyChartsSource, EC_DDL
from data_platform.ingest import _local_midnight_ms


EC_TYPES = [
    "Solar", "Wind onshore", "Wind offshore", "Load",
    "Hydro Run-of-River", "Hydro water reservoir", "Hydro pumped storage",
    "Hydro pumped storage consumption", "Biomass", "Geothermal", "Waste",
    "Others", "Fossil brown coal / lignite", "Fossil hard coal",
    "Fossil oil", "Fossil coal-derived gas", "Fossil gas",
    "Cross border electricity trading", "Residual load",
    "Renewable share of load", "Renewable share of generation",
]



def ingest_energy_charts(
        con: duckdb.DuckDBPyConnection,
        source: FixtureEnergyChartsSource,
        start: date,
        end: date,
        types: Sequence[str] = EC_TYPES,) -> int:
    """(Re)load bronze_energy_charts from the source for [start, end) in local time. Idempotent."""

    con.execute(EC_DDL)  # get Table     
    start_ms = _local_midnight_ms(start)
    end_ms   = _local_midnight_ms(end)
    start_utc = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_utc   = datetime.fromtimestamp(end_ms   / 1000, tz=timezone.utc)    

    cols = list(types)
    df = source.load_df()[cols].reset_index()
    df = df[(df["ts_utc"] >= start_utc) & (df["ts_utc"] < end_utc)]
    
    
    long = df.melt(id_vars="ts_utc", var_name="production_type", value_name="power_mw").dropna(subset=["power_mw"])
    long["country"] = source.country
    
    con.execute("BEGIN")
    try:
        con.execute("""
        DELETE FROM bronze_energy_charts
        WHERE country = ? AND ts_utc >= ? AND ts_utc < ?
        """, [source.country, start_utc, end_utc])
        con.register("ec_df", long)
        con.execute("""
        INSERT OR REPLACE INTO bronze_energy_charts (ts_utc, country, production_type, power_mw)
        SELECT ts_utc, country, production_type, power_mw FROM ec_df
        """)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    
    return len(long)
