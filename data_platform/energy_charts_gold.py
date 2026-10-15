from collections import defaultdict
from datetime import date
from typing import Optional

import duckdb

from data_platform.ingest import LOCAL_TZ
from data_platform.quality_patched import expected_hours
from utils import settings
import pandas as pd


GOLD_DDL = """
CREATE OR REPLACE TABLE gold_daily_energy (
    local_date              DATE PRIMARY KEY,

    -- SMARD (canonical)
    consumption_mwh         DOUBLE,
    pv_mwh                  DOUBLE,
    wind_onshore_mwh        DOUBLE,
    wind_offshore_mwh       DOUBLE,
    renewable_share         DOUBLE,
    is_complete             BOOLEAN NOT NULL,

    -- EC enrichment (types not covered by SMARD)
    hydro_ror_mwh           DOUBLE,
    hydro_reservoir_mwh     DOUBLE,
    hydro_pumped_mwh        DOUBLE,
    pumped_storage_consumption_mwh DOUBLE,
    biomass_mwh             DOUBLE,
    geothermal_mwh          DOUBLE,
    waste_mwh               DOUBLE,
    others_mwh              DOUBLE,
    lignite_mwh             DOUBLE,
    hard_coal_mwh           DOUBLE,
    oil_mwh                 DOUBLE,
    coal_gas_mwh            DOUBLE,
    gas_mwh                 DOUBLE,
    cross_border_mwh        DOUBLE
);
"""


EC_TO_GOLD = {
    "Hydro Run-of-River":          "hydro_ror_mwh",
    "Hydro water reservoir":       "hydro_reservoir_mwh",
    "Hydro pumped storage":        "hydro_pumped_mwh",
    "Hydro pumped storage consumption": "pumped_storage_consumption_mwh",    
    "Biomass":                     "biomass_mwh",
    "Geothermal":                  "geothermal_mwh",
    "Waste":                       "waste_mwh",
    "Others":                      "others_mwh",
    "Fossil brown coal / lignite": "lignite_mwh",
    "Fossil hard coal":            "hard_coal_mwh",
    "Fossil oil":                  "oil_mwh",
    "Fossil coal-derived gas":     "coal_gas_mwh",
    "Fossil gas":                  "gas_mwh",
    "Cross border electricity trading": "cross_border_mwh"
}


EC_SKIP = {
    "Solar", "Wind onshore", "Wind offshore",   # already in SMARD
    "Load", "Residual load",                     # replicates/derivatives
    "Renewable share of load",                   # derivativ
    "Renewable share of generation",             # derivative    
}



EC_COLUMNS = list(EC_TO_GOLD.values())


def _smard_daily(con, f_netz, f_pv, f_won, f_woff):
    rows = con.execute("""
    SELECT CAST(ts AS DATE) AS local_date, filter_id,
    SUM(value) AS mwh, count(*) AS hours
    FROM bronze_smard
    WHERE resolution = 'hour' AND value IS NOT NULL
    AND filter_id IN (?, ?, ?, ?)
    GROUP BY 1, 2
    """, [f_netz, f_pv, f_won, f_woff]).fetchall()

    by_day = defaultdict(dict)
    for d, fid, mwh, hours in rows:
        by_day[d][fid] = (mwh, hours)
    return by_day


def _ec_daily(con):
    placeholders = ",".join("?" * len(EC_TO_GOLD))
    df = con.execute(f"""
        SELECT ts_utc, production_type, power_mw
        FROM bronze_energy_charts
        WHERE production_type IN ({placeholders})
    """, list(EC_TO_GOLD)).df()

    if df.empty:
        return {}

    df["local_date"] = (
        pd.to_datetime(df["ts_utc"], utc=True)
          .dt.tz_convert("Europe/Berlin")
          .dt.date
    )
    df["mwh"] = df["power_mw"] * 0.25

    daily = df.groupby(["local_date", "production_type"], as_index=False)["mwh"].sum()

    by_day = defaultdict(dict)
    for _, r in daily.iterrows():
        by_day[r["local_date"]][EC_TO_GOLD[r["production_type"]]] = r["mwh"]
    return by_day




def build_gold_daily_energy(con, tz=LOCAL_TZ) -> int:
    con.execute(GOLD_DDL)

    filters = settings.data_platform.smard_filters
    f_netz = int(filters["netzlast"])
    f_pv   = int(filters["photovoltaik"])
    f_won  = int(filters["wind_onshore"])
    f_woff = int(filters["wind_offshore"])
    wanted = (f_netz, f_pv, f_won, f_woff)

    smard = _smard_daily(con, *wanted)
    ec    = _ec_daily(con)

    out = []
    for d in sorted(set(smard) | set(ec)):
        g = smard.get(d, {})
        cons = g.get(f_netz, (None, 0))[0]
        pv   = g.get(f_pv,   (None, 0))[0]
        won  = g.get(f_won,  (None, 0))[0]
        woff = g.get(f_woff, (None, 0))[0]

        renewables = [x for x in (pv, won, woff) if x is not None]
        renew = sum(renewables) if renewables else None
        share = (renew / cons) if (renew is not None and cons) else None

        complete = all(
            g.get(fid, (None, 0))[1] == expected_hours(d, tz) for fid in wanted
        )

        ec_cols = ec.get(d, {})
        row = (
            d, cons, pv, won, woff, share, complete,
            *[ec_cols.get(c) for c in EC_COLUMNS],
        )
        out.append(row)

    n_cols = 7 + len(EC_COLUMNS)
    placeholders = ",".join("?" * n_cols)
    if out:
        con.executemany(
            f"INSERT OR REPLACE INTO gold_daily_energy VALUES ({placeholders})",
            out,
        )
    return len(out)
