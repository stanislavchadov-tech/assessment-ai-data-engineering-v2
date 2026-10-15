from datetime import date, datetime
from zoneinfo import ZoneInfo
import pytest

from data_platform.gold import build_gold_daily_energy as build_gold_daily_energy_smard
from data_platform.energy_charts_gold import build_gold_daily_energy as build_gold_daily_energy_ec, EC_TO_GOLD
from data_platform.ingest import connect, ingest_range
from data_platform.energy_charts_ingest import ingest_energy_charts
from data_platform.smard_client import FixtureSmardSource
from data_platform.energy_charts import FixtureEnergyChartsSource

import sys


NETZLAST, PV, WIND_ON, WIND_OFF = 410, 4068, 4067, 1225
DST_START_WEEK = (date(2026, 3, 23), date(2026, 3, 30))   # 29-03 : 23h
REGULAR_WEEK   = (date(2026, 3, 30), date(2026, 4, 6))

@pytest.fixture()
def con():
    connection = connect(":memory:")
    yield connection
    connection.close()

def _load_all(con, start, end):
    src = FixtureSmardSource()
    for fid in (NETZLAST, PV, WIND_ON, WIND_OFF):
        ingest_range(con, src, fid, start, end)
    ingest_energy_charts(con, FixtureEnergyChartsSource(), start, end)
    
def _gold(con, day):
    return con.execute(
        "SELECT * FROM gold_daily_energy WHERE local_date = ?", [day]
    ).fetchone()


# --- SMARD + DST (at it was) ---

def test_dst_start_day_is_complete_23h(con):
    _load_all(con, *DST_START_WEEK)
    build_gold_daily_energy_smard(con)
    row = _gold(con, date(2026, 3, 29))
    assert row is not None
    cols = [c[0] for c in con.execute("DESCRIBE gold_daily_energy").fetchall()]
    assert row[cols.index("is_complete")] is True


def test_dst_day_not_flagged_incomplete(con):
    _load_all(con, *DST_START_WEEK)
    build_gold_daily_energy_smard(con)
    total = con.execute("SELECT count(*) FROM gold_daily_energy").fetchone()[0]
    complete = con.execute(
        "SELECT count(*) FROM gold_daily_energy WHERE is_complete"
    ).fetchone()[0]
    assert complete == total


# --- EC enrichment ---

def test_ec_columns_present(con):
    """ if EC_TO_GOLD is contained in cols from gold_daily_energy"""
    _load_all(con, *DST_START_WEEK)
    build_gold_daily_energy_ec(con)
    cols = {c[0] for c in con.execute("DESCRIBE gold_daily_energy").fetchall()}
    assert set(EC_TO_GOLD.values()) <= cols 


def test_ec_values_on_dst_day(con):
    _load_all(con, *DST_START_WEEK)
    build_gold_daily_energy_ec(con)
    row = con.execute("""
        SELECT hydro_ror_mwh, biomass_mwh,
               pumped_storage_consumption_mwh, cross_border_mwh
        FROM gold_daily_energy WHERE local_date = DATE '2026-03-29'
    """).fetchone()
    assert row is not None
    assert row[0] is not None and row[0] > 0          # hydro RoR > 0
    assert row[1] is not None and row[1] > 0          # biomass > 0
    assert row[2] is not None and row[2] <= 0         # pumped consumption <= 0

    
def test_ec_no_duplicate_of_smard(con):
    """Solar/wind/Load from EC should not appear as separate columns ."""
    _load_all(con, *DST_START_WEEK)
    build_gold_daily_energy_ec(con)
    cols = {c[0] for c in con.execute("DESCRIBE gold_daily_energy").fetchall()}
    forbidden = {
        "pv_ec_mwh", "wind_on_ec_mwh", "wind_off_ec_mwh", "load_ec_mwh",
        "solar_mwh", "load_mwh",
    }
    assert not (forbidden & cols)


# --- Idempotency / rebuild ---

def test_rebuild_is_idempotent(con):
    _load_all(con, *DST_START_WEEK)
    build_gold_daily_energy_ec(con)
    a = con.execute("SELECT * FROM gold_daily_energy ORDER BY local_date").fetchall()
    build_gold_daily_energy_ec(con)
    b = con.execute("SELECT * FROM gold_daily_energy ORDER BY local_date").fetchall()
    assert a == b

    
# --- DST for EC aggregation ---

def test_ec_dst_day_has_92_slots(con):
    _load_all(con, *DST_START_WEEK)
    tz = ZoneInfo("Europe/Berlin")
    start = datetime(2026, 3, 29, 0, 0, 0, tzinfo=tz)
    end   = datetime(2026, 3, 30, 0, 0, 0, tzinfo=tz)
    n = con.execute("""
        SELECT count(*) FROM bronze_energy_charts
        WHERE production_type = 'Solar'
          AND ts_utc >= ? AND ts_utc < ?
    """, [start, end]).fetchone()[0]
    assert n == 92
