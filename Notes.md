# NOTES

## B1 — three bugs, one root

**#1 naive `ts` in PK.** `_to_ts` used `datetime.fromtimestamp(ms/1000)` without tz →
naive in the *system* zone; offset/fold lost. `PK(...,ts)` then collapsed the two
`02:00` on 26 Oct 2025 → 24 h instead of 25. Evidence: INSERT of two equal naive
`02:00` → `ConstraintException: duplicate key`. Before fix, `_hours_on(2025-10-26)`
= 24 (Berlin) / 23 (UTC) / 24 (Tokyo) → depends on the system zone (the "differs
between machine and CI" case).
**Fix:** `ts_utc TIMESTAMPTZ` in PK; `_to_ts(tz=LOCAL_TZ)`; `_fetch_rows` stores
`ts_utc = aware.astimezone(utc)` and `ts = aware.replace(tzinfo=None)`; `_write` 8 `?`.

**#2 backfill wiped other weeks.** `ingest_range` deleted the whole series
(`WHERE filter_id/region/resolution`), so reloading one week erased its neighbours.
**Fix:** delete only reloaded chunks — `chunk_ts IN (?,…)`.

**#3 incremental missed late hours.** SMARD back-fills the latest chunk; the
selection `c > watermark` excluded it. **Fix:** `c >= watermark` (safe: `INSERT OR
REPLACE` by `ts_utc`).

**Review `quality.py`.** `incomplete_days` uses constant 24 → false alerts on DST
days (23/25) and on the partial current day; scans all history. Proposed fix (not
applied to prod code): `quality_patched.py` — `expected_hours(day, tz)`, `since`,
`skip_today`.

**Review `cli.py`.** `quality` inherits the bug; `source` built even for offline
`quality`; no `--from<--to` check; `incremental` drops `--since`; one failing series
aborts the loop; no summary/exit code for schedulers.

## B2 — gold_daily_energy

One row per local day: `local_date` PK, `consumption_mwh`, `pv_mwh`, `wind_onshore_mwh`,
`wind_offshore_mwh`, `renewable_share`, `is_complete`. Built from bronze by
`build_gold_daily_energy(con, tz=LOCAL_TZ)`; `CREATE OR REPLACE TABLE` + INSERT →
rebuildable and idempotent. Completeness uses `expected_hours(day, tz)` (23/24/25),
not 24 — the same helper as `quality.py`, directly avoiding the B1 mistake.

Tests (`test_gold.py`): `test_dst_{end,start}_day_is_complete_*` catch #1;
`test_dst_day_not_flagged_incomplete` catches the constant-24 bug;
`test_rebuild_is_idempotent` catches #2; TZ matrix catches the system-zone bug.
Other tests cover formula, daily SUM, rebuild-from-bronze. `pytest` green in
Berlin / UTC / Tokyo.

## B3 — second source (Energy-Charts)

**Policy.** SMARD is canonical. EC contributes only what SMARD lacks (hydro, biomass,
lignite, gas, cross-border, …); duplicates are not stored as columns, overlaps are
**reported as anomalies**, never persisted.

**Implementation.** `FixtureEnergyChartsSource` (15-min, MW, `load_df()` cached);
`bronze_energy_charts(ts_utc, country, production_type, power_mw)` PK → idempotent;
`ingest_energy_charts` range-filters, range-DELETE, vector insert (executemany was
minutes → <2 s). Gold extended with 14 EC types, `SUM(power_mw*0.25)` per Berlin day.

**Findings.** EC is derived from SMARD. Week 2026-03-23..30: Solar ≈ 0.0004%,
Wind offshore ≈ 2e-06% (rounding); Wind onshore ≈ 1.9146%, std 2.3e-07 → systematic
offset ⇒ a fixed % threshold is meaningless.

**`reconcile_report(base_till, check_since, k=5, min_pct=0.1, min_baseline_days=10)`.**
Baseline `< base_till`, check `>= check_since`; anomaly = `|diff − base| > max(k·MAD,
min_pct)`; short/empty baseline → warning + skip. Writes nothing to gold.
Verified: Oct baseline vs Mar check flags everything (different periods, not
anomalies); clean Mar baseline + `Solar×5` on 03-25 → exactly one `diff_shift`.

**AT TIME ZONE trap.** DuckDB’s `AT TIME ZONE` uses the *system* zone; under `TZ=UTC`
the DST slot count was 84 instead of 92 — same class as B1. Fix: pandas
`tz_convert("Europe/Berlin")` / aware datetime params.

**Tests.** EC columns present; no duplicate columns; EC DST = 92 slots; rebuild
idempotent; reconcile clean/corrupted/warning. TZ matrix green.

## Not done
Fill-ins for missing SMARD series; revision detection; CLI `reconcile`; continuous
fixture for a realistic baseline; `cli.py` review items.

## AI notes
- **Helped:** DST PK diagnosis; `ts_utc` + explicit tz; gold aggregation; reconcile.
- **Corrected/rejected:** `aware.timezone` → `astimezone`; `chunk_ts = ?` + `*chunks`
  → `IN (…)`; rejected edits to `tests/`; dropped `renewable_mwh`; hard-coded DST
  dates → `expected_hours`; SQL `AT TIME ZONE` → pandas `tz_convert`.
- **Verified:** in-memory repros with ROLLBACK; `date -d` for epoch constants;
  `pytest` under Berlin/UTC/Tokyo; bronze/gold counts on DST days (23/25 h).
  
