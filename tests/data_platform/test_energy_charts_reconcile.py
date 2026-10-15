from datetime import date
import pytest

from data_platform.ingest import connect, ingest_range
from data_platform.energy_charts import FixtureEnergyChartsSource
from data_platform.energy_charts_ingest import ingest_energy_charts
from data_platform.smard_client import FixtureSmardSource
from data_platform.energy_charts_reconcile import reconcile_report


NETZLAST, PV, WIND_ON, WIND_OFF = 410, 4068, 4067, 1225
FIXTURE_START = date(2025, 10, 20)
FIXTURE_END   = date(2026, 3, 30)


@pytest.fixture()
def con():
    connection = connect(":memory:")
    src_smard = FixtureSmardSource()
    for fid in (NETZLAST, PV, WIND_ON, WIND_OFF):
        ingest_range(connection, src_smard, fid, FIXTURE_START, FIXTURE_END)
    ingest_energy_charts(connection, FixtureEnergyChartsSource(),
                         FIXTURE_START, FIXTURE_END)
    yield connection
    connection.close()




def test_reconcile_detects_corrupted_day(con):
    con.execute("""
        UPDATE bronze_energy_charts SET power_mw = power_mw * 5
        WHERE production_type = 'Solar'
          AND ts_utc >= TIMESTAMPTZ '2026-03-25 00:00:00+01:00'
          AND ts_utc <  TIMESTAMPTZ '2026-03-26 00:00:00+01:00'
    """)
    anomalies, _ = reconcile_report(
        con,
        base_since=date(2026,3,23),
        base_till=date(2026, 3, 25),    # baseline: 23, 24 (clean)
        check_since=date(2026, 3, 25),  # check: from 25
        min_baseline_days=2,
    )
    pv_shifts = [a for a in anomalies if a.series == "pv" and a.kind == "diff_shift"]
    assert len(pv_shifts) == 1
    assert pv_shifts[0].local_date == date(2026, 3, 25)


def test_reconcile_insufficient_baseline_warns(con):
    anomalies, warnings = reconcile_report(
        con,        
        base_since=date(2026,3,23), base_till=date(2026, 3, 25), check_since=date(2026, 3, 25),
        min_baseline_days=10,
    )
    assert any("insufficient baseline" in w for w in warnings)
    assert anomalies == []
    assert [a for a in anomalies if a.kind == "diff_shift"] == []
    
