from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional
from zoneinfo import ZoneInfo

import duckdb

from utils import settings

LOCAL_TZ = ZoneInfo(settings.data_platform.LOCAL_TIMEZONE)

def expected_hours(local_day: date, tz: ZoneInfo = LOCAL_TZ) -> int:
    """Number of local hours in a calendar day (24, or 23/25 on DST transitions).
    Computed from the zone, not hard-coded: the transition dates move every year.
    """
    UTC = ZoneInfo("UTC")
    start = datetime(local_day.year, local_day.month, local_day.day, tzinfo=tz)
    nxt = local_day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=tz)
    return int((end.astimezone(UTC) - start.astimezone(UTC)).total_seconds() // 3600)


def incomplete_days( con: duckdb.DuckDBPyConnection,
                     filter_id: int,
                     tz: ZoneInfo = LOCAL_TZ,
                     since: Optional[date] = None,
                     skip_today: bool=True,) -> List[Tuple[date, int]]:
    """Days whose hourly series is not complete. Feeds the morning alert mail.

    Compares each local day's hour count against the *expected* count for that
    day (24, or 23/25 on DST days) instead of a constant 24, so DST days do not
    raise false alerts.
    """
    
    today = datetime.now(tz).date()
    
    rows = con.execute(
        """
        SELECT local_date, count(*) AS hours
        FROM silver_smard_hourly
        WHERE filter_id = ?
        GROUP BY local_date
        ORDER BY local_date
        """,
        [filter_id],
    ).fetchall()
    
    out = []
    for d,n in rows:
        if since and d < since:
            continue
        if skip_today and d==today:
            continue
        if n != expected_hours(d, tz):
            out.append((d,n))    
    return out
