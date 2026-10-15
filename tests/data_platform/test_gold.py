from datetime import date

import pytest

from data_platform.gold import GOLD_DDL, build_gold_daily_energy
from data_platform.ingest import connect, ingest_incremental, ingest_range
from data_platform.smard_client import FixtureSmardSource

NETZLAST = 410
PV = 4068
WIND_ON = 4067
WIND_OFF = 1225

DST_END_WEEK = (date(2025, 10, 20), date(2025, 10, 27))   # 26.10.2025: 25 hours
DST_START_WEEK = (date(2026, 3, 23), date(2026, 3, 30))   # 29.03.2026: 23 hours
REGULAR_WEEK = (date(2026, 3, 30), date(2026, 4, 6))      # 168 hours



@pytest.fixture()
def con():
    connection = connect(":memory:")
    yield connection
    connection.close()


def _ingest_week(con, start, end):
    """Re/Loads all four sources for the week"""
    src = FixtureSmardSource()
    for fid in (NETZLAST, PV, WIND_ON, WIND_OFF):
        ingest_range(con,src,fid,start,end)

def _gold_row(con,day):
    """ Reads the row for a day """
    return con.execute(
        """SELECT local_date, mwh_consumption, mwh_pv, mwh_wind_onshore,
        mwh_wind_offshore, renewable_share, is_complete
        FROM gold_daily_energy WHERE local_date = ?""",
        [day],
    ).fetchone()



def test_dst_end_day_is_complete_25h(con):
    """2025-10-26 (25 hours) must be complete. Alerts on naive constant 24."""
    _ingest_week(con, *DST_END_WEEK)
    build_gold_daily_energy(con)
    row = _gold_row(con, date(2025, 10, 26))
    assert row is not None
    cols = [c[0] for c in con.execute("DESCRIBE gold_daily_energy").fetchall()]
    assert row[cols.index("is_complete")] is True


def test_dst_start_day_is_complete_23h(con):
    """2026-03-29 (23 hours) must be complete. Alerts on naive constant 24."""
    _ingest_week(con, *DST_START_WEEK)
    build_gold_daily_energy(con)
    row = _gold_row(con, date(2026, 3, 29))
    assert row is not None
    cols = [c[0] for c in con.execute("DESCRIBE gold_daily_energy").fetchall()]
    assert row[cols.index("is_complete")] is True

    
def test_regular_day_is_complete(con):
    _ingest_week(con, *REGULAR_WEEK)
    build_gold_daily_energy(con)
    row = _gold_row(con, date(2026, 4, 1))   # ordinary day
    cols = [c[0] for c in con.execute("DESCRIBE gold_daily_energy").fetchall()]
    assert row[cols.index("is_complete")] is True


def test_missing_hour_marks_incomplete(con):
    """Really missing (no DST) -> is_complete=False."""
    _ingest_week(con, *REGULAR_WEEK)
    # drop one hour in some source
    con.execute(
        """DELETE FROM bronze_smard
           WHERE filter_id = ? AND ts = TIMESTAMP '2026-04-01 12:00:00'""",
        [PV],
    )
    build_gold_daily_energy(con)
    row = _gold_row(con, date(2026, 4, 1))
    cols = [c[0] for c in con.execute("DESCRIBE gold_daily_energy").fetchall()]
    assert row[cols.index("is_complete")] is False, "missing hour must mark the day incomplete"

    
def test_dst_day_not_flagged_incomplete(con):
    """Analogy of the  quality-bug: DST-day should not marked as incomplete."""
    _ingest_week(con, *DST_END_WEEK)
    build_gold_daily_energy(con)
    complete_days = con.execute(
        "SELECT count(*) FROM gold_daily_energy WHERE is_complete"
    ).fetchone()[0]
    # the whole week must be complete
    total_days = con.execute("SELECT count(*) FROM gold_daily_energy").fetchone()[0]
    assert complete_days == total_days
    
    
# --- Idempotency / rebuild ---

def test_rebuild_is_idempotent(con):
    _ingest_week(con, *DST_END_WEEK)
    build_gold_daily_energy(con)
    first = con.execute(
        "SELECT * FROM gold_daily_energy ORDER BY local_date"
    ).fetchall()

    build_gold_daily_energy(con)
    second = con.execute(
        "SELECT * FROM gold_daily_energy ORDER BY local_date"
    ).fetchall()

    assert first == second
    assert len(first) == len(second)

    
def test_rebuildable_from_bronze(con):
    """gold can be deleted and derived from bronze again."""
    _ingest_week(con, *DST_END_WEEK)
    build_gold_daily_energy(con)
    before = con.execute(
        "SELECT * FROM gold_daily_energy ORDER BY local_date"
    ).fetchall()

    con.execute("DELETE FROM gold_daily_energy")
    build_gold_daily_energy(con)
    after = con.execute(
        "SELECT * FROM gold_daily_energy ORDER BY local_date"
    ).fetchall()

    assert before == after


# --- Semantics ---

def test_renewable_share_formula(con):
    _ingest_week(con, *REGULAR_WEEK)
    build_gold_daily_energy(con)
    row = _gold_row(con, date(2026, 4, 1))
    _, cons, pv, won, woff, share, _ = row
    renew = pv + won + woff
    expected = renew / cons
    assert abs(share - expected) < 1e-9


def test_consumption_is_daily_sum(con):
    """mwh_consumption = sum of the hour-resolved NETZLAST over the day."""
    _ingest_week(con, *REGULAR_WEEK)
    build_gold_daily_energy(con)
    row = _gold_row(con, date(2026, 4, 1))
    _, cons, *_ = row

    db_sum = con.execute(
        """SELECT SUM(value) FROM bronze_smard
           WHERE filter_id = ? AND CAST(ts AS DATE) = DATE '2026-04-01'""",
        [NETZLAST],
    ).fetchone()[0]
    assert abs(cons - db_sum) < 1e-9

    
    
if __name__ == "__main__":

    import duckdb
    from utils import settings
    
    con = duckdb.connect(settings.data_platform.DUCKDB_PATH)
    print(con.execute("""
    SELECT CAST(ts AS DATE) AS d, filter_id, count(*)
    FROM bronze_smard
    WHERE filter_id IN (410,4068,4067,1225)
    AND CAST(ts AS DATE) BETWEEN DATE '2026-03-28' AND DATE '2026-03-30'
    GROUP BY 1,2 ORDER BY 1,2
    """).fetchall()
          )
          
