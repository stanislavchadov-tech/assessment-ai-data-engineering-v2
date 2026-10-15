from dataclasses import dataclass
from datetime import date
from statistics import median
import duckdb
import pandas as pd


@dataclass
class Anomaly:
    local_date: date
    series: str      # 'pv' | 'wind_onshore' | 'wind_offshore' | 'consumption'
    kind: str        # 'diff_shift' | 'missing' | 'stale'
    detail: str


SMARD_EC_PAIRS = {
    "pv":            (4068, "Solar"),
    "wind_onshore":  (4067, "Wind onshore"),
    "wind_offshore": (1225, "Wind offshore"),
    "consumption":   (410,  "Load"),
}


def _smard_daily(con, fid):
     exists = con.execute("""
        SELECT CAST(ts AS DATE) AS d, SUM(value)
        FROM bronze_smard
        WHERE filter_id = ? AND resolution = 'hour' AND value IS NOT NULL
        GROUP BY 1
    """, [fid]).fetchall()
     if not exists:
         return {}
     return dict(exists)


def _ec_daily(con, pt):
    df = con.execute("""
        SELECT ts_utc, power_mw FROM bronze_energy_charts
        WHERE production_type = ?
    """, [pt]).df()
    if df.empty:
        return {}
    df["local_date"] = (
        pd.to_datetime(df["ts_utc"], utc=True)
          .dt.tz_convert("Europe/Berlin")
          .dt.date
    )
    df["mwh"] = df["power_mw"] * 0.25
    return dict(df.groupby("local_date")["mwh"].sum())



def reconcile_report(con, base_since, base_till, check_since=None, k=5.0, min_pct=0.1, min_baseline_days=2):
    if check_since is None:
        check_since = base_till
    out = []
    warnings=[]

    for series, (fid, pt) in SMARD_EC_PAIRS.items():
        
        smard = _smard_daily(con, fid)
        ec    = _ec_daily(con, pt)

        # --- baseline: all up to base_till ---
        base_diffs = []
        for d in sorted(set(smard) & set(ec)):
            if base_since is not None and d < base_since:
                continue
            if d >= base_till:
                continue
            if smard[d]:
                base_diffs.append((smard[d] - ec[d]) / smard[d] * 100)

        if not base_diffs:
            warnings.append(f'no baseline data: fid={fid}, base_till={base_till}')
            continue
            

        if len(base_diffs) < min_baseline_days:
            warnings.append(f"insufficient baseline: series={series} fid={fid} "
                            f"days={len(base_diffs)} < {min_baseline_days} (base_till={base_till})"
                            )
            continue
        
        base = median(base_diffs)
        mad  = median(abs(v - base) for v in base_diffs) 
        threshold = max(k * mad, min_pct)

        # --- check: from check_since ---
        for d in sorted(set(smard) & set(ec)):
            if d < check_since:
                continue
            if not smard[d]:
                continue
            diff = (smard[d] - ec[d]) / smard[d] * 100
            if abs(diff - base) > threshold:
                out.append(Anomaly(d, series, "diff_shift",
                                   f"diff={diff:.3f}% base={base:.3f}% thr={threshold:.3f}"))

        # --- missing: from сheck_since ---
        for d in sorted(set(smard) - set(ec)):
            if d >= check_since:
                out.append(Anomaly(d, series, "missing", "no EC data"))

    return out, warnings
