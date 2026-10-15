"""Load SMARD time series into the bronze layer (DuckDB) and expose a silver view."""

from datetime import date, datetime, time, timedelta, timezone
from typing import List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import duckdb

from utils import get_logger, settings

from .smard_client import SmardSource

logger = get_logger(__name__)

LOCAL_TZ = ZoneInfo(settings.data_platform.LOCAL_TIMEZONE)

DDL = """
CREATE TABLE IF NOT EXISTS bronze_smard (
    filter_id   INTEGER   NOT NULL,
    region      VARCHAR   NOT NULL,
    resolution  VARCHAR   NOT NULL,
    ts_utc      TIMESTAMPTZ NOT NULL,
    ts          TIMESTAMP NOT NULL,
    value       DOUBLE,
    chunk_ts    BIGINT    NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    PRIMARY KEY (filter_id, region, resolution, ts_utc)
);

CREATE TABLE IF NOT EXISTS ingest_watermark (
    filter_id     INTEGER NOT NULL,
    region        VARCHAR NOT NULL,
    resolution    VARCHAR NOT NULL,
    last_chunk_ts BIGINT  NOT NULL,
    PRIMARY KEY (filter_id, region, resolution)
);

CREATE OR REPLACE VIEW silver_smard_hourly AS
SELECT
    filter_id,
    region,
    ts,
    CAST(ts AS DATE) AS local_date,
    value
FROM bronze_smard
WHERE resolution = 'hour' AND value IS NOT NULL;
"""

Row = Tuple[int, str, str, datetime, datetime, float, int, datetime]


def connect(path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path or settings.data_platform.DUCKDB_PATH)
    con.execute(DDL)
    return con


def _local_midnight_ms(day: date) -> int:
    return int(datetime.combine(day, time(0), tzinfo=LOCAL_TZ).timestamp() * 1000)


def _to_ts(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=LOCAL_TZ)


def _chunks_overlapping(index: Sequence[int], start_ms: int, end_ms: int) -> List[int]:
    ordered = sorted(index)
    ends = ordered[1:] + [2**62]
    return [c for c, nxt in zip(ordered, ends) if c < end_ms and nxt > start_ms]


def _fetch_rows(
    source: SmardSource, filter_id: int, region: str, resolution: str, chunks: Sequence[int]
) -> List[Row]:
    now = datetime.now(timezone.utc)
    rows: List[Row] = []
    for chunk_ts in chunks:
        for ts_ms, value in source.chunk(filter_id, region, resolution, chunk_ts):
            if value is None:
                continue
            aware = _to_ts(ts_ms)
            ts_utc = aware.astimezone(timezone.utc)
            ts = aware.replace(tzinfo=None)
            rows.append((filter_id, region, resolution, ts_utc, ts, value, chunk_ts, now))
    return rows


def _write(con: duckdb.DuckDBPyConnection, rows: Sequence[Row]) -> None:
    if rows:
        con.executemany(
            "INSERT OR REPLACE INTO bronze_smard VALUES (?, ?, ?, ?, ?, ?, ?, ?)", list(rows)
        )


def _set_watermark(con, filter_id: int, region: str, resolution: str, chunk_ts: int) -> None:
    con.execute(
        """
        INSERT INTO ingest_watermark VALUES (?, ?, ?, ?)
        ON CONFLICT (filter_id, region, resolution)
        DO UPDATE SET last_chunk_ts = greatest(last_chunk_ts, excluded.last_chunk_ts)
        """,
        [filter_id, region, resolution, chunk_ts],
    )


def ingest_range(
    con: duckdb.DuckDBPyConnection,
    source: SmardSource,
    filter_id: int,
    start: date,
    end: date,
    *,
    region: str = "DE",
    resolution: str = "hour",
) -> int:
    """(Re)load all chunks overlapping [start, end) in local time. Used for backfills."""
    chunks = _chunks_overlapping(
        source.index(filter_id, region, resolution), _local_midnight_ms(start), _local_midnight_ms(end)
    )
    rows = _fetch_rows(source, filter_id, region, resolution, chunks)
   
    con.execute("BEGIN")
    try:
        if chunks:
            placeholders = ",".join("?" * len(chunks))
            con.execute(
                f"DELETE FROM bronze_smard WHERE filter_id = ? AND region = ? AND resolution = ? AND chunk_ts IN ({placeholders})",                
                [filter_id, region, resolution, *chunks],
            )
            _write(con, rows)
            _set_watermark(con, filter_id, region, resolution, max(chunks))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    logger.info("range load filter=%s chunks=%d rows=%d", filter_id, len(chunks), len(rows))
    return len(rows)


def ingest_incremental(
    con: duckdb.DuckDBPyConnection,
    source: SmardSource,
    filter_id: int,
    *,
    region: str = "DE",
    resolution: str = "hour",
    since: Optional[date] = None,
) -> int:
    """Load everything that is new since the last run. Scheduled hourly.

    Without a watermark, starts at ``since`` (default: INCREMENTAL_BOOTSTRAP_DAYS ago).
    """
    found = con.execute(
        "SELECT last_chunk_ts FROM ingest_watermark WHERE filter_id = ? AND region = ? AND resolution = ?",
        [filter_id, region, resolution],
    ).fetchone()
    watermark = found[0] if found else None
    
    index = source.index(filter_id, region, resolution)
    if watermark is None:
        if since is None:
            since = datetime.now(LOCAL_TZ).date() - timedelta(
                days=int(settings.data_platform.INCREMENTAL_BOOTSTRAP_DAYS)
            )
        chunks = _chunks_overlapping(index, _local_midnight_ms(since), 2**62)
    else:
        chunks = [c for c in index if c >= watermark]
    rows = _fetch_rows(source, filter_id, region, resolution, chunks)

    con.execute("BEGIN")
    try:
        _write(con, rows)
        if chunks:
            _set_watermark(con, filter_id, region, resolution, max(chunks))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    
    logger.info("incremental filter=%s chunks=%d rows=%d", filter_id, len(chunks), len(rows))
    return len(rows)
