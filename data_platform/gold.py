from collections import defaultdict
from datetime import date
from typing import Optional

import duckdb

from data_platform.ingest import LOCAL_TZ
from data_platform.quality_patched import expected_hours
from utils import settings


GOLD_DDL = """
CREATE OR REPLACE TABLE gold_daily_energy (
    local_date         DATE PRIMARY KEY,
    mwh_consumption    DOUBLE,
    mwh_pv             DOUBLE,
    mwh_wind_onshore   DOUBLE,
    mwh_wind_offshore  DOUBLE,
    renewable_share    DOUBLE,
    is_complete        BOOLEAN NOT NULL
);
"""


def build_gold_daily_energy(con: duckdb.DuckDBPyConnection, tz=LOCAL_TZ) -> int:

    con.execute(GOLD_DDL)
    
    out=[]
    
    filters = settings.data_platform.smard_filters # {'netzlast': 410, 'photovoltaik': 4068, 'wind_onshore': 4067, 'wind_offshore': 1225}
    f_netz = filters["netzlast"]
    f_pv = filters["photovoltaik"]
    f_won = filters["wind_onshore"]
    f_wof = filters["wind_offshore"]
    #print(f_netz,f_pv,f_won,f_wof)

    rows = con.execute(
        """
        SELECT CAST (ts AS DATE) AS local_date, filter_id, SUM(value) AS mwh, count(*) AS hours
        FROM bronze_smard
        WHERE resolution = 'hour' AND value IS NOT NULL AND filter_id IN (?,?,?,?)
        GROUP BY 1, 2
        """,
        [f_netz,f_pv,f_won,f_wof],
    ).fetchall()

    by_day  = defaultdict(dict)

    for d,fid,mhw,hours in rows:
        by_day[d][fid]  = (mhw, hours)
        
    for d in sorted(by_day):
        g = by_day[d]
        mwh_consumption       = g.get(f_netz,(None,0))[0]  # [0] - mwh, [1] - hours
        hours_consumption = g.get(f_netz,(None,0))[1]
        mwh_pv   = g.get(f_pv,(None,0))[0]
        hours_pv = g.get(f_pv,(None,0))[1]
        mwh_won   = g.get(f_won,(None,0))[0]
        hours_won = g.get(f_won,(None,0))[1]
        mwh_wof   = g.get(f_wof,(None,0))[0]
        hours_wof = g.get(f_wof,(None,0))[1]
        #print(hours_consumption, hours_pv, hours_won, hours_wof)        
        renew_share = 0
        renewables = [x for x in (mwh_pv, mwh_won, mwh_wof) if x is not None]
        renew_share, is_complete  = None, False
        if mwh_consumption and renewables:
            renew_share = (mwh_pv + mwh_won + mwh_wof)  / mwh_consumption
            is_complete = all([ expected_hours(d, LOCAL_TZ) == hours_consumption,
                              expected_hours(d, LOCAL_TZ) == hours_pv,
                              expected_hours(d, LOCAL_TZ) == hours_won,
                              expected_hours(d, LOCAL_TZ) == hours_wof])            
        print(d, " consumption:%12.3f  pv:%12.3f  won:%12.3f  wof:%12.3f   renew_share: %.3f  "  % (mwh_consumption, mwh_pv, mwh_won, mwh_wof, renew_share), "is_complete:", is_complete)
        
        out.append((d, mwh_consumption, mwh_pv, mwh_won, mwh_wof, renew_share, is_complete))
            
    con.execute("DELETE FROM gold_daily_energy")
    if out:
        con.executemany("INSERT INTO gold_daily_energy VALUES (?, ?, ?, ?, ?, ?, ?)", out)
       
    return len(out)


if __name__ == "__main__":

    # delete the old if exists
    #con = duckdb.connect(settings.data_platform.DUCKDB_PATH)
    #con.execute('DROP TABLE IF EXISTS gold_daily_energy')
    #con.close()

    # make a new one
    con = duckdb.connect(settings.data_platform.DUCKDB_PATH)
    n = build_gold_daily_energy(con, tz=LOCAL_TZ)
    print('total days:',n)
