import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response

import export
from calculator import compute_etp, compute_etp_from_summary, compute_etp_hourly, compute_etp_intraday
from database import Database
from stations import WeatherStationListener, create_listener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

GIT_SHA = os.environ.get("GIT_SHA", "unknown")
logger.info("Starting weather-station-etp, commit=%s", GIT_SHA)

LATITUDE          = float(os.environ.get("LATITUDE", "48.8566"))
# Not used by the ET0 formula itself (only latitude + elevation are) -
# only needed by station types that look up a location-indexed external
# dataset (stations/meteofrance.py).
LONGITUDE         = float(os.environ.get("LONGITUDE", "2.3522"))
ELEVATION         = float(os.environ.get("ELEVATION", "100"))
ANEMOMETER_HEIGHT = float(os.environ.get("ANEMOMETER_HEIGHT", "2.0"))
DATA_DIR          = os.environ.get("DATA_DIR", "/data")
# How long raw per-minute observations are kept at full resolution before
# being downsampled into hourly min/max/mean buckets (kept indefinitely —
# see Database.archive_old_observations). Default 1 year, so any ET0 method
# can be recomputed at full precision over real history instead of only
# starting from whenever it was added; beyond that, hourly buckets still
# carry enough signal (min/max temp, mean RH/wind/solar) for every method
# here, and for longer-term climate/irrigation trend analysis.
OBSERVATION_RETENTION_HOURS = int(os.environ.get("OBSERVATION_RETENTION_HOURS", "8760"))
# Comma-separated list of "type" or "type:instance" entries; the first one
# is the API default. Bare "type" (e.g. "tempest") is a single, unnamed
# instance - identical to earlier single-station deployments, same DB
# filename, no migration needed. "type:instance" (e.g.
# "pwsweather:HALOLVANTAGE,pwsweather:OTHERSTATION") lets several stations
# of the same vendor coexist, each with its own DB/ETP history; `instance`
# is passed to the driver for instance-specific identification where that's
# already meaningful (e.g. PWSWeather's station id). Receiving two
# concurrent Tempest or Ecowitt instances still needs listener-sharing work
# the drivers don't have yet (both would try to bind the same local
# UDP/HTTP port) - this only wires up the addressing/DB side so it never
# needs another migration once that lands.
def _parse_station_entries(raw: str) -> list[tuple[str, str, str | None]]:
    """Return (instance_key, station_type, instance_id) tuples, de-duplicated by instance_key."""
    entries, seen = [], set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        # Only the type is case-normalised - the instance id (e.g. a PWS
        # station id) must keep its original case for drivers that send it
        # on to a case-sensitive API.
        station_type, _, instance_id = item.partition(":")
        station_type = station_type.lower()
        instance_id = instance_id or None
        instance_key = station_type if instance_id is None else f"{station_type}-{instance_id.lower()}"
        if instance_key in seen:
            continue
        seen.add(instance_key)
        entries.append((instance_key, station_type, instance_id))
    return entries

STATION_ENTRIES = _parse_station_entries(os.environ.get("STATION_TYPE", "tempest"))

try:
    TZ = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Paris"))
except ZoneInfoNotFoundError:
    logger.warning("Unknown TIMEZONE=%r, falling back to UTC", os.environ.get("TIMEZONE"))
    TZ = ZoneInfo("UTC")



def _db_path(instance_key: str) -> str:
    return f"{DATA_DIR}/observations-{instance_key}.db"


# Pre-multi-station deployments used a single observations.db: hand it to the
# first configured station so its ETP history is kept.
_legacy_db = f"{DATA_DIR}/observations.db"
_first_key = STATION_ENTRIES[0][0]
if os.path.exists(_legacy_db) and not os.path.exists(_db_path(_first_key)):
    logger.info("Migrating %s to %s", _legacy_db, _db_path(_first_key))
    os.rename(_legacy_db, _db_path(_first_key))

# One database (observations + ETP) per station instance.
stations: dict[str, WeatherStationListener] = {
    instance_key: create_listener(station_type, Database(_db_path(instance_key)), instance_id=instance_id)
    for instance_key, station_type, instance_id in STATION_ENTRIES
}
for st in stations.values():
    st.default_latitude, st.default_elevation, st.anemometer_height = LATITUDE, ELEVATION, ANEMOMETER_HEIGHT
    st.default_longitude = LONGITUDE
    st.observation_retention_hours = OBSERVATION_RETENTION_HOURS
sched = BackgroundScheduler()


def _station(name: str | None) -> WeatherStationListener:
    if name is None:
        return stations[_first_key]
    try:
        return stations[name.lower()]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Unknown station {name!r}") from None


def run_etp():
    for name, st in stations.items():
        if not st.computes_own_etp:
            continue  # e.g. meteofrance: refreshed on its own schedule in _run(), nothing to compute here
        latitude = st.latitude if st.latitude is not None else LATITUDE
        elevation = st.elevation if st.elevation is not None else ELEVATION
        try:
            result = compute_etp(st.db, latitude, elevation, ANEMOMETER_HEIGHT)
            if result:
                st.db.store_etp(result)
                alt = ", ".join(f"{m}={v:.3f}" for m, v in result["et0_alt_mm"].items())
                logger.info("[%s] ETP penman_monteith=%.3f mm/day (%d obs) %s",
                            name, result["et0_mm"], result["observation_count"], alt)
            else:
                logger.warning("[%s] Insufficient observations for ETP computation", name)
        except Exception:
            logger.exception("[%s] ETP computation failed", name)

        # True single-hour ET0 (FAO-56 hourly Penman-Monteith, 1h window) -
        # a separate computation/table from the rolling-24h one above, which
        # is a *daily* rate just recomputed every hour, not a measure of
        # that hour alone. Kept in its own try so a failure here never blocks
        # the rolling-24h value the rest of the app (Today's big number,
        # daily chart, history) depends on.
        try:
            longitude = st.longitude if st.longitude is not None else LONGITUDE
            # Snap to the top of the hour so cron, startup and backfill rows
            # all share the same hour_end (store_etp_hourly upserts on it).
            hour_end = int(time.time()) // 3600 * 3600
            hourly = compute_etp_hourly(st.db, latitude, longitude, elevation, ANEMOMETER_HEIGHT, TZ,
                                        now=hour_end)
            if hourly:
                st.db.store_etp_hourly(hourly)
                logger.info("[%s] Hourly ETP penman_monteith=%.3f mm (%d obs)",
                            name, hourly["et0_mm"], hourly["observation_count"])
            else:
                logger.warning("[%s] Insufficient observations for hourly ETP computation", name)
        except Exception:
            logger.exception("[%s] Hourly ETP computation failed", name)


def run_etp_hourly_backfill():
    """One-shot catch-up for etp_hourly_results: Tempest/Ecowitt already keep
    raw per-minute history going back up to OBSERVATION_RETENTION_HOURS, so
    every past hour's true ET0 can be computed locally right away instead of
    only building up one new row per real hour that passes after this
    feature shipped - otherwise Today's hourly/accumulated charts would
    start out empty and only fill in live, one hour at a time. Skips hours
    already stored, so a restart after the first run is a cheap no-op."""
    now_ts = int(time.time())
    for name, st in stations.items():
        if not st.computes_own_etp:
            continue
        earliest_day = st.db.earliest_observation_day()
        if not earliest_day:
            continue
        earliest_ts = int(datetime(earliest_day.year, earliest_day.month, earliest_day.day,
                                    tzinfo=timezone.utc).timestamp())
        start_ts = max(earliest_ts, now_ts - OBSERVATION_RETENTION_HOURS * 3600)
        existing = {r["hour_end"] for r in st.db.get_etp_hourly_range(start_ts, now_ts)}
        latitude = st.latitude if st.latitude is not None else LATITUDE
        longitude = st.longitude if st.longitude is not None else LONGITUDE
        elevation = st.elevation if st.elevation is not None else ELEVATION
        filled = 0
        hour_end = (start_ts // 3600 + 1) * 3600
        while hour_end <= now_ts:
            if hour_end not in existing:
                try:
                    result = compute_etp_hourly(st.db, latitude, longitude, elevation,
                                                 ANEMOMETER_HEIGHT, TZ, now=hour_end)
                    if result:
                        st.db.store_etp_hourly(result)
                        filled += 1
                except Exception:
                    logger.exception("[%s] Hourly ETP backfill failed for hour_end=%d", name, hour_end)
            hour_end += 3600
        if filled:
            logger.info("[%s] Backfilled %d hourly ETP row(s)", name, filled)


def run_archive():
    for name, st in stations.items():
        written = st.db.archive_old_observations(keep_hours=OBSERVATION_RETENTION_HOURS)
        if written:
            logger.info("[%s] Archived %d hourly bucket(s) of raw observations", name, written)


def run_pwsweather_summary_refresh():
    """Once a day's complete, refine its ETP using Xweather's own QC'd daily
    summary (temp/RH/wind/dewpoint/solar max/min/avg from their full data
    stream for the station) instead of leaving it at whatever the hourly
    cron derived from our own locally-polled samples. Targets the exact
    same window_start/window_end the hourly hits at local midnight
    (export.local_day_bounds), so store_etp's upsert-by-window_end replaces
    that day's row in place rather than competing with it. Covers a few
    trailing days, not just yesterday, to ride out a late/failed Xweather
    publish or a missed run (safe to re-run: same upsert)."""
    for name, st in stations.items():
        if not hasattr(st, "fetch_daily_summary"):
            continue  # only PWSWeatherListener has a summary endpoint to call
        lat = st.latitude if st.latitude is not None else LATITUDE
        elev = st.elevation if st.elevation is not None else ELEVATION
        for days_ago in range(1, 4):
            day = date.today() - timedelta(days=days_ago)
            try:
                summary = st.fetch_daily_summary(day)
                if not summary:
                    continue
                result = compute_etp_from_summary(summary, lat, elev, ANEMOMETER_HEIGHT, day)
                if not result:
                    logger.warning("[%s] %s: summary present but missing required fields", name, day)
                    continue
                window_start, window_end = export.local_day_bounds(day, TZ)
                st.db.store_etp({**result, "window_start": window_start, "window_end": window_end})
                logger.info("[%s] %s: refined ETP from Xweather daily summary: %.3f mm/day",
                            name, day, result["et0_mm"])
            except Exception:
                logger.exception("[%s] Daily summary refresh failed for %s", name, day)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for st in stations.values():
        st.db.init()
        st.start()
    sched.add_job(run_etp,     "cron", minute=0)       # top of every hour
    sched.add_job(run_archive, "cron", hour=3, minute=0)  # daily downsample + purge
    sched.add_job(run_pwsweather_summary_refresh, "cron", hour=1, minute=30)
    sched.add_job(run_pwsweather_summary_refresh, "date", run_date=datetime.now())  # once, in the background
    sched.add_job(run_etp_hourly_backfill, "date", run_date=datetime.now())  # once, in the background
    sched.start()
    run_etp()   # immediate first computation on startup
    yield
    sched.shutdown(wait=False)
    for st in stations.values():
        st.stop()


app = FastAPI(title="Weather Station ETP", lifespan=lifespan)


def _iso(ts: int | None) -> str | None:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None


def _station_status(st: WeatherStationListener) -> dict:
    obs = st.db.get_latest_observation()
    # Falls back to the listener's own last_seen for a driver with no raw
    # observations at all (stations/meteofrance.py) - otherwise it would
    # always read as disconnected even right after a successful refresh.
    last_ts = obs["timestamp"] if obs else (int(st.last_seen) if st.last_seen else None)
    return {
        "station_type": st.name,
        "station_connected": last_ts is not None and (time.time() - last_ts) < st.stale_after,
        "last_observation_at": _iso(last_ts),
    }


@app.get("/api/stations")
def list_stations():
    return {"stations": [_station_status(st) for st in stations.values()]}


@app.get("/health")
def health(station: str | None = None):
    st = _station(station)
    return {
        "status": "ok",
        "commit": GIT_SHA,
        **_station_status(st),
        "latitude": st.latitude if st.latitude is not None else LATITUDE,
        "longitude": st.longitude if st.longitude is not None else LONGITUDE,
        "elevation_m": st.elevation if st.elevation is not None else ELEVATION,
        "location_source": "station" if st.latitude is not None else "env",
        "anemometer_height_m": ANEMOMETER_HEIGHT,
        "observation_retention_days": round(OBSERVATION_RETENTION_HOURS / 24, 1),
        # Present only for drivers that resolve an external location-indexed
        # grid point (stations/meteofrance.py) - surfaced mainly so a
        # longitude that silently fell back to the default above (e.g. not
        # learned from an API, and LONGITUDE not set) is visible here rather
        # than only showing up as a wrong-looking grid point in the data.
        **({"meteofrance_grid_point": st.grid_point} if getattr(st, "grid_point", None) else {}),
    }


@app.get("/api/etp/current")
def current_etp(station: str | None = None):
    r = _station(station).db.get_latest_etp()
    if not r:
        return JSONResponse(status_code=503, content={"error": "No ETP data yet"})
    return {
        "window_start":     _iso(r["window_start"]),
        "window_end":       _iso(r["window_end"]),
        "et0_mm_per_day":   r["et0_mm"],
        "et0_alt_mm_per_day": r["et0_alt_mm"],
        "observation_count": r["observation_count"],
    }


@app.get("/api/etp/history")
def etp_history(limit: int = 48, station: str | None = None):
    rows = _station(station).db.get_etp_history(limit)
    return {
        "entries": [
            {
                "window_start":     _iso(r["window_start"]),
                "window_end":       _iso(r["window_end"]),
                "et0_mm_per_day":   r["et0_mm"],
                "et0_alt_mm_per_day": r["et0_alt_mm"],
                "observation_count": r["observation_count"],
            }
            for r in rows
        ]
    }


@app.get("/api/etp/daily")
def etp_daily(station: str | None = None, start: date | None = None, end: date | None = None):
    """One point per local calendar day: the single rolling-24h computation whose
    window spans that day end-to-end (local midnight to midnight) - the FAO-56 /
    Meteo France convention of one ETo per calendar day from that day's actual
    data. Averaging every hourly computation that happened to run during the day
    instead mixes in trailing hours from the previous day and smears real
    day-to-day swings into the number (etp_results is never purged, so this
    covers the station's whole history, not just the last 49h of raw
    observations). `start`/`end` (local calendar dates, inclusive) optionally
    restrict the range."""
    if start and end and end < start:
        raise HTTPException(status_code=400, detail="end date must be on or after start date")
    start_ts = export.local_day_bounds(start, TZ)[0] if start else None
    end_ts = export.local_day_bounds(end, TZ)[1] if end else None
    rows = _station(station).db.get_etp_range(start_ts, end_ts)

    # Pick whichever computation's window_end lands closest to local midnight
    # for each calendar day - normally an exact hit, since the hourly cron
    # ticks on local-hour boundaries (UTC and local always differ by a whole
    # number of hours). No cutoff: a missed tick, a restart-triggered
    # off-schedule run, or drift still gives the closest available estimate
    # for that day rather than silently dropping it.
    best = _etp_by_day(rows)

    return {
        "days": [
            {
                "date":                day.isoformat(),
                "et0_mm":              r["et0_mm"],
                "et0_alt_mm":          r["et0_alt_mm"],
                "observation_count":   r["observation_count"],
                "midnight_offset_min": round(dist / 60, 1),
            }
            for day, (dist, r) in sorted(best.items())
        ]
    }


@app.get("/api/etp/intraday")
def etp_intraday(date: date, station: str | None = None):
    """Cumulative ET0 "through hour h" across the given local calendar day, by
    method - the Research intraday chart and Today's accumulation curve both
    use this (Today just ignores every method but Penman-Monteith). Backs
    both a finalized past day (full curve, hour 1..24, ending exactly at that
    day's stored et0_mm/et0_alt_mm) and today-in-progress (curve stops at the
    current local hour, ending at the latest rolling-24h estimate). Only
    produces points for stations with raw per-minute observations (Tempest,
    Ecowitt) - a daily-summary-only station (Meteo France, PWSWeather) has no
    finer-than-a-day data to build a same-day curve from, so `hours` comes
    back empty, same as a day with no stored ET0 to distribute at all."""
    st = _station(station)
    today_local = datetime.now(TZ).date()
    if date > today_local:
        raise HTTPException(status_code=400, detail="date cannot be in the future")
    day_start_ts, day_end_ts = export.local_day_bounds(date, TZ)
    finalized = date < today_local
    query_end_ts = day_end_ts if finalized else min(day_end_ts, int(time.time()))
    max_hour = 24 if finalized else max(0, min(24, (query_end_ts - day_start_ts + 3599) // 3600))

    if finalized:
        rows = st.db.get_etp_range(day_start_ts - 3600, day_end_ts)
        match = _etp_by_day(rows).get(date)
        targets = match[1] if match else {}
    else:
        targets = st.db.get_latest_etp() or {}

    obs = st.db.get_range(day_start_ts, query_end_ts)
    hours = compute_etp_intraday(obs, day_start_ts, targets, max_hour)
    return {"date": date.isoformat(), "finalized": finalized, "hours": hours}


@app.get("/api/etp/hourly")
def etp_hourly(date: date, station: str | None = None):
    """Each hour's own true ET0 (mm, FAO-56 hourly Penman-Monteith over that
    hour's 1h window - calculator.compute_etp_hourly/run_etp) across the
    given local calendar day. Unlike /api/etp/intraday (which distributes a
    *known daily total* across hours by solar share), these are independent
    per-hour computations - so, unlike that endpoint, this one is never
    monotonic/cumulative and a given hour can come back null if there wasn't
    enough data in it, regardless of neighbouring hours."""
    st = _station(station)
    today_local = datetime.now(TZ).date()
    if date > today_local:
        raise HTTPException(status_code=400, detail="date cannot be in the future")
    day_start_ts, day_end_ts = export.local_day_bounds(date, TZ)
    rows = st.db.get_etp_hourly_range(day_start_ts, day_end_ts)

    # hour N is the period ending at local clock hour N (1..24) - same
    # convention as etp_intraday's `hour` field, so both can share the chart
    # x-axis/renderer on the frontend.
    by_hour = {}
    for r in rows:
        h = round((r["hour_end"] - day_start_ts) / 3600)
        if 1 <= h <= 24:
            by_hour[h] = r["et0_mm"]

    return {
        "date": date.isoformat(),
        "hours": [{"hour": h, "et0_mm": by_hour.get(h)} for h in range(1, 25)],
    }


@app.get("/api/observations/daily-summary")
def daily_summary(date: date, station: str | None = None):
    """A single local calendar day's finalized weather aggregates (min/max/mean
    per dimension) alongside its ET0 - feeds Today's number+chips and
    Research's intraday numeric panel. `aggregates` is None if the station has
    no raw or archived data at all for that day (e.g. it was offline, or - for
    a daily-summary-only station - Xweather/Meteo France hasn't published yet)."""
    st = _station(station)
    today_local = datetime.now(TZ).date()
    if date > today_local:
        raise HTTPException(status_code=400, detail="date cannot be in the future")
    start_ts, end_ts = export.local_day_bounds(date, TZ)
    aggregates = st.db.get_daily_aggregate(start_ts, end_ts)

    # Narrow window around this one day is enough for _etp_by_day to find its
    # match (window_end normally lands exactly on end_ts) without scanning the
    # station's whole etp_results history - padded an hour on each side so a
    # missed tick or a late/off-schedule run (window_end short of, or just
    # past, local midnight) is still found, same tolerance etp_daily gets
    # from scanning its whole requested range.
    rows = st.db.get_etp_range(start_ts - 3600, end_ts + 3600)
    match = _etp_by_day(rows).get(date)
    et0_mm = match[1]["et0_mm"] if match else None

    return {
        "date": date.isoformat(),
        "finalized": date < today_local,
        "et0_mm": et0_mm,
        "aggregates": aggregates,
    }


def _etp_by_day(rows: list[dict]) -> dict[date, tuple[int, dict]]:
    """Pick whichever computation's window_end lands closest to local midnight
    for each calendar day its window_end belongs to (window_end's own day,
    minus the day it represents - see etp_daily's docstring). Returns
    {day: (dist_seconds, row)} so callers can also inspect how close the match
    was."""
    best: dict[date, tuple[int, dict]] = {}
    for r in rows:
        day = datetime.fromtimestamp(r["window_end"], tz=TZ).date() - timedelta(days=1)
        target = export.local_day_bounds(day, TZ)[1]
        dist = abs(r["window_end"] - target)
        if day not in best or dist < best[day][0]:
            best[day] = (dist, r)
    return best


def _parse_date_range(start: date, end: date) -> tuple[int, int]:
    if end < start:
        raise HTTPException(status_code=400, detail="end date must be on or after start date")
    start_ts, _ = export.local_day_bounds(start, TZ)
    _, end_ts = export.local_day_bounds(end, TZ)
    return start_ts, end_ts


MAX_BROWSE_ROWS = 2000


@app.get("/api/observations/range")
def observations_range(start: date, end: date, station: str | None = None):
    """Raw observations in a date range, for the dashboard's data browser.
    Capped at MAX_BROWSE_ROWS (most recent within the range); use
    /api/export/range for the complete CSV."""
    start_ts, end_ts = _parse_date_range(start, end)
    rows = _station(station).db.get_range(start_ts, end_ts)
    total = len(rows)
    rows = rows[-MAX_BROWSE_ROWS:]
    return {
        "total": total,
        "rows": [
            {
                "timestamp":           _iso(r["timestamp"]),
                "temperature_c":       r["temperature"],
                "humidity_pct":        r["humidity"],
                "wind_speed_ms":       r["wind_speed"],
                "solar_radiation_wm2": r["solar_radiation"],
                "pressure_hpa":        r["pressure"],
            }
            for r in rows
        ],
    }


@app.get("/api/observations/archive")
def observations_archive(start: date, end: date, station: str | None = None):
    """Archived hourly min/max/mean buckets in a date range, for the dashboard's
    data browser. Capped at MAX_BROWSE_ROWS; use /api/export/archive for the
    complete CSV."""
    start_ts, end_ts = _parse_date_range(start, end)
    rows = _station(station).db.get_archive_range(start_ts, end_ts)
    total = len(rows)
    rows = rows[-MAX_BROWSE_ROWS:]
    return {
        "total": total,
        "rows": [
            {
                "hour_start":          _iso(r["hour_start"]),
                "temperature_min_c":   r["temperature_min"],
                "temperature_max_c":   r["temperature_max"],
                "temperature_mean_c":  r["temperature_mean"],
                "humidity_min_pct":    r["humidity_min"],
                "humidity_max_pct":    r["humidity_max"],
                "humidity_mean_pct":   r["humidity_mean"],
                "wind_speed_min_ms":   r["wind_speed_min"],
                "wind_speed_max_ms":   r["wind_speed_max"],
                "wind_speed_mean_ms":  r["wind_speed_mean"],
                "solar_radiation_min_wm2":  r["solar_radiation_min"],
                "solar_radiation_max_wm2":  r["solar_radiation_max"],
                "solar_radiation_mean_wm2": r["solar_radiation_mean"],
                "pressure_min_hpa":    r["pressure_min"],
                "pressure_max_hpa":    r["pressure_max"],
                "pressure_mean_hpa":   r["pressure_mean"],
                "sample_count":        r["sample_count"],
            }
            for r in rows
        ],
    }


@app.get("/api/observations/latest")
def latest_obs(station: str | None = None):
    obs = _station(station).db.get_latest_observation()
    if not obs:
        return JSONResponse(status_code=503, content={"error": "No observations yet"})
    return {
        "timestamp":            _iso(obs["timestamp"]),
        "temperature_c":        obs["temperature"],
        "humidity_pct":         obs["humidity"],
        "wind_speed_ms":        obs["wind_speed"],
        "solar_radiation_wm2":  obs["solar_radiation"],
        "pressure_hpa":         obs["pressure"],
    }


def _csv_response(rows: list[dict], filename: str, columns=export.COLUMNS) -> Response:
    return Response(
        content=export.rows_to_csv(rows, TZ, columns),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/daily")
def export_daily(station: str | None = None):
    """CSV of yesterday's observations, local midnight to midnight."""
    st = _station(station)
    yesterday = datetime.now(TZ).date() - timedelta(days=1)
    start_ts, end_ts = export.local_day_bounds(yesterday, TZ)
    rows = st.db.get_range(start_ts, end_ts)
    return _csv_response(rows, f"weather-station-{st.name}-{yesterday.isoformat()}.csv")


@app.get("/api/export/range")
def export_range(start: date, end: date, station: str | None = None):
    """CSV of observations between two local calendar dates (inclusive)."""
    st = _station(station)
    start_ts, end_ts = _parse_date_range(start, end)
    rows = st.db.get_range(start_ts, end_ts)
    return _csv_response(rows, f"weather-station-{st.name}-{start.isoformat()}_to_{end.isoformat()}.csv")


@app.get("/api/export/archive")
def export_archive(start: date, end: date, station: str | None = None):
    """CSV of archived hourly min/max/mean buckets (beyond OBSERVATION_RETENTION_HOURS,
    raw per-minute data is downsampled into these rather than deleted)."""
    st = _station(station)
    start_ts, end_ts = _parse_date_range(start, end)
    rows = st.db.get_archive_range(start_ts, end_ts)
    return _csv_response(
        rows, f"weather-station-{st.name}-archive-{start.isoformat()}_to_{end.isoformat()}.csv",
        columns=export.ARCHIVE_COLUMNS,
    )


@app.get("/", response_class=HTMLResponse)
def index():
    return _HTML


_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Weather Station ETP</title>
  <style>
    /* class rules like .badge-inprogress {display:block} would otherwise
       override the hidden attribute */
    [hidden] { display:none !important; }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body  { font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0; padding: 2rem; }
    h1    { font-size: 1.4rem; color: #7dd3fc; margin-bottom: 1.5rem; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
    .card { background: #1e293b; border-radius: 0.75rem; padding: 1.25rem; }
    .lbl  { font-size: 0.7rem; text-transform: uppercase; letter-spacing: .06em; color: #94a3b8; margin-bottom: .4rem; }
    .val  { font-size: 2rem; font-weight: 700; }
    .unit { font-size: .85rem; color: #94a3b8; margin-left: .2rem; }
    .sub  { font-size: .7rem; color: #64748b; margin-top: .3rem; }
    .dot  { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; }
    .ok   { background:#22c55e; } .err { background:#ef4444; }
    h2    { font-size: .9rem; color: #94a3b8; margin-bottom: .6rem; }
    table { width:100%; border-collapse:collapse; background:#1e293b; border-radius:.75rem; overflow:hidden; }
    th,td { padding:.65rem 1rem; text-align:left; font-size:.8rem; }
    th    { background:#0f172a; color:#94a3b8; font-weight:600; }
    tr:not(:last-child) td { border-bottom:1px solid #0f172a; }
    .tabs { display:flex; gap:.5rem; margin-bottom:1.5rem; flex-wrap:wrap; }
    .tab  { padding:.45rem 1rem; border-radius:.4rem; border:none; background:#1e293b; color:#94a3b8;
            font-weight:600; cursor:pointer; text-transform:capitalize; }
    .tab.active { background:#7dd3fc; color:#0f172a; }

    .section-nav { display:flex; gap:.5rem; margin-bottom:1.5rem; }
    .section-btn { flex:1; padding:.7rem 1rem; border:none; border-radius:.5rem; background:#1e293b;
                   color:#94a3b8; font-weight:700; font-size:.95rem; cursor:pointer; }
    .section-btn.active { background:#7dd3fc; color:#0f172a; }

    .day-stepper { display:flex; align-items:center; justify-content:center; gap:1rem; margin-bottom:1.25rem; }
    .step-btn { width:48px; height:48px; border-radius:50%; border:none; background:#1e293b; color:#7dd3fc;
                font-size:1.4rem; cursor:pointer; }
    .step-btn:disabled { opacity:.35; cursor:default; }
    .day-label { font-size:1rem; font-weight:600; min-width:10rem; text-align:center; }

    .badge-inprogress { display:block; width:fit-content; margin:0 auto 1.25rem; background:#334155;
                         color:#fbbf24; font-size:.75rem; font-weight:600; padding:.4rem .9rem;
                         border-radius:999px; text-align:center; }
    .state-msg { text-align:center; color:#64748b; padding:2.5rem 0; }
    .state-msg button { margin-left:.6rem; }

    .today-main { display:flex; flex-direction:column; gap:1.5rem; }
    .today-et0  { font-size:3rem; font-weight:800; }
    .today-chips { display:flex; gap:.75rem; flex-wrap:wrap; margin-top:1.25rem; }
    .chip { background:#1e293b; border-radius:.6rem; padding:.85rem 1rem; flex:1; min-width:120px; text-align:center; }
    .chip-icon { font-size:1.3rem; display:block; }
    .chip-val  { font-size:1.05rem; font-weight:700; display:block; margin-top:.3rem; }
    .chip-sub  { font-size:.65rem; color:#64748b; display:block; margin-top:.1rem; }
    .chip-lbl  { font-size:.65rem; color:#94a3b8; text-transform:uppercase; letter-spacing:.05em; }
    #today-chart, #today-hourly-chart, #intraday-chart { width:100%; }
    .chart-lbl { font-size:.75rem; color:#94a3b8; margin:0 0 .4rem; }

    @media (max-width: 767px) {
      body { padding-bottom: 5.5rem; }
      .section-nav { position:fixed; left:0; right:0; bottom:0; margin:0; z-index:50;
                      padding:.6rem 1rem calc(.6rem + env(safe-area-inset-bottom));
                      background:#1e293b; box-shadow:0 -2px 10px rgba(0,0,0,.4); }
      .section-btn { padding:.9rem .5rem; border-radius:.6rem; min-height:48px; }
    }
    @media (min-width: 768px) {
      .today-main { flex-direction:row; align-items:flex-start; }
      .today-left { flex:1; max-width:320px; }
      .today-right { flex:2; }
    }
  </style>
</head>
<body>
  <h1>Weather Station ETP Monitor</h1>
  <nav class="section-nav" id="section-nav">
    <button class="section-btn" data-section="today">Today</button>
    <button class="section-btn" data-section="research">Research</button>
  </nav>

  <section id="section-today">
    <div class="day-stepper" id="today-stepper">
      <button class="step-btn" id="today-prev" aria-label="Previous day">‹</button>
      <div class="day-label" id="today-label">—</div>
      <button class="step-btn" id="today-next" aria-label="Next day">›</button>
    </div>
    <div class="badge-inprogress" id="today-badge" hidden>Still building up — check back after midnight</div>
    <div class="state-msg" id="today-loading" hidden>Loading…</div>
    <div class="state-msg" id="today-empty" hidden>No data for this day</div>
    <div class="state-msg" id="today-error" hidden>Couldn't load data for this day. <button class="tab" id="today-retry">Retry</button></div>
    <div class="today-main" id="today-content" hidden>
      <div class="today-left">
        <div class="today-et0" id="today-et0">—<span class="unit">mm/day</span></div>
        <div class="today-chips" id="today-chips">
          <div class="chip"><span class="chip-icon">☀️</span><span class="chip-val" id="chip-solar">—</span><span class="chip-lbl">Solar</span></div>
          <div class="chip"><span class="chip-icon">🌡️</span><span class="chip-val" id="chip-temp">—</span><span class="chip-sub" id="chip-temp-range"></span><span class="chip-lbl">Temp</span></div>
          <div class="chip"><span class="chip-icon">💧</span><span class="chip-val" id="chip-hum">—</span><span class="chip-lbl">Humidity</span></div>
          <div class="chip"><span class="chip-icon">💦</span><span class="chip-val" id="chip-dew">—</span><span class="chip-lbl">Dew Point</span></div>
          <div class="chip"><span class="chip-icon">🍃</span><span class="chip-val" id="chip-wind">—</span><span class="chip-lbl">Wind</span></div>
        </div>
      </div>
      <div class="today-right">
        <p class="chart-lbl">Accumulated ET0 since midnight (running total of each hour's own value)</p>
        <canvas id="today-chart" height="240"></canvas>
        <p class="chart-lbl" style="margin-top:1.25rem">Hourly ET0 (each hour's own FAO-56 Penman-Monteith, 1h window)</p>
        <canvas id="today-hourly-chart" height="200"></canvas>
      </div>
    </div>
  </section>

  <section id="section-research" hidden>
  <nav class="tabs" id="tabs" hidden></nav>
  <nav class="tabs" id="research-tabs">
    <button class="tab active" data-view="monitor">Monitor</button>
    <button class="tab" data-view="daily">Daily ETP Chart</button>
    <button class="tab" data-view="intraday">Intraday</button>
    <button class="tab" data-view="browse">Browse Data</button>
  </nav>

  <div id="view-monitor">
  <div class="grid">
    <div class="card">
      <div class="lbl">ETP</div>
      <div class="val" id="etp">—<span class="unit">mm/day</span></div>
      <div class="sub" id="etp-sub">awaiting data…</div>
    </div>
    <div class="card">
      <div class="lbl">Temperature</div>
      <div class="val" id="temp">—<span class="unit">°C</span></div>
    </div>
    <div class="card">
      <div class="lbl">Humidity</div>
      <div class="val" id="hum">—<span class="unit">%</span></div>
    </div>
    <div class="card">
      <div class="lbl">Solar Radiation</div>
      <div class="val" id="sol">—<span class="unit">W/m²</span></div>
    </div>
    <div class="card">
      <div class="lbl">Wind Speed</div>
      <div class="val" id="wind">—<span class="unit">m/s</span></div>
    </div>
    <div class="card">
      <div class="lbl">Station</div>
      <div class="val" style="font-size:1rem;padding-top:.4rem">
        <span class="dot err" id="dot"></span><span id="conn">checking…</span>
      </div>
      <div class="sub" id="last-seen"></div>
    </div>
  </div>

  <h2>Compare ETP Methods — current window</h2>
  <table style="margin-bottom:2rem">
    <thead><tr><th>Method</th><th>ETP mm/day</th><th>Δ vs Penman-Monteith</th></tr></thead>
    <tbody id="methods"><tr><td colspan="3" style="color:#64748b">loading…</td></tr></tbody>
  </table>

  <h2>ETP History — last 48 entries</h2>
  <table>
    <thead><tr><th>Period</th><th>ETP mm/day</th><th>Observations</th></tr></thead>
    <tbody id="hist"><tr><td colspan="3" style="color:#64748b">loading…</td></tr></tbody>
  </table>

  <h2 style="margin-top:2rem">Export Data (CSV)</h2>
  <div class="card">
    <a href="/api/export/daily" id="export-daily" style="color:#7dd3fc">Download yesterday's data</a>
    <form action="/api/export/range" method="get" style="margin-top:1rem;display:flex;gap:.75rem;align-items:flex-end;flex-wrap:wrap">
      <label style="font-size:.75rem;color:#94a3b8">From<br>
        <input type="date" name="start" required style="margin-top:.3rem">
      </label>
      <label style="font-size:.75rem;color:#94a3b8">To<br>
        <input type="date" name="end" required style="margin-top:.3rem">
      </label>
      <input type="hidden" name="station" id="export-station">
      <button type="submit" style="padding:.45rem 1rem;border-radius:.4rem;border:none;background:#7dd3fc;color:#0f172a;font-weight:600;cursor:pointer">Download raw CSV</button>
      <button type="submit" formaction="/api/export/archive" style="padding:.45rem 1rem;border-radius:.4rem;border:none;background:#334155;color:#e2e8f0;cursor:pointer">Download archived hourly CSV</button>
    </form>
    <div class="sub" style="margin-top:.5rem">Raw covers the last ~<span id="retention-days">?</span> days; archive holds downsampled hourly min/max/mean for everything older.</div>
  </div>

  <h2 style="margin-top:2rem">Settings</h2>
  <div class="card">
    <label style="display:flex;align-items:center;gap:.5rem;font-size:.85rem;color:#94a3b8;cursor:pointer">
      <input type="checkbox" id="pin-today-toggle">
      Always open on Today
    </label>
    <div class="sub" style="margin-top:.4rem">Mainly for farmer-facing installs — skips remembering the last section visited and always lands on Today.</div>
  </div>
  </div>

  <div id="view-daily" hidden>
  <h2>Daily ETP by Method</h2>
  <div class="card" style="margin-bottom:2rem">
    <form id="daily-range" style="display:flex;gap:.75rem;align-items:flex-end;flex-wrap:wrap;margin-bottom:1rem">
      <label style="font-size:.75rem;color:#94a3b8">From<br>
        <input type="date" name="start" style="margin-top:.3rem">
      </label>
      <label style="font-size:.75rem;color:#94a3b8">To<br>
        <input type="date" name="end" style="margin-top:.3rem">
      </label>
      <button type="submit" style="padding:.45rem 1rem;border-radius:.4rem;border:none;background:#7dd3fc;color:#0f172a;font-weight:600;cursor:pointer">Apply</button>
      <button type="button" id="daily-range-clear" style="padding:.45rem 1rem;border-radius:.4rem;border:none;background:#334155;color:#e2e8f0;cursor:pointer">Full history</button>
    </form>
    <div class="tabs" id="daily-mode" style="margin-bottom:.75rem">
      <button type="button" class="tab active" data-mode="absolute">Absolute values</button>
      <button type="button" class="tab" data-mode="delta">Delta vs Penman-Monteith</button>
    </div>
    <canvas id="daily-chart" height="260" style="width:100%;max-width:100%"></canvas>
    <div id="daily-legend" style="display:flex;gap:1rem;flex-wrap:wrap;margin-top:.75rem;font-size:.75rem;color:#94a3b8"></div>
    <div class="sub" style="margin-top:.5rem">Each point is the single rolling-24h computation whose window spans that calendar day, local midnight to midnight.</div>
  </div>
  </div>

  <div id="view-intraday" hidden>
  <h2>Intraday / Accumulation</h2>
  <div class="day-stepper" id="intraday-stepper">
    <button class="step-btn" id="intraday-prev" aria-label="Previous day">‹</button>
    <div class="day-label" id="intraday-label">—</div>
    <button class="step-btn" id="intraday-next" aria-label="Next day">›</button>
  </div>
  <div class="badge-inprogress" id="intraday-badge" hidden>Still building up — check back after midnight</div>
  <div class="state-msg" id="intraday-loading" hidden>Loading…</div>
  <div class="state-msg" id="intraday-empty" hidden>No intraday data for this station/day</div>
  <div class="state-msg" id="intraday-error" hidden>Couldn't load data for this day. <button class="tab" id="intraday-retry">Retry</button></div>
  <div id="intraday-content" hidden>
    <div class="card" style="margin-bottom:1.5rem">
      <canvas id="intraday-chart" height="280"></canvas>
      <div id="intraday-legend" style="display:flex;gap:1rem;flex-wrap:wrap;margin-top:.75rem;font-size:.75rem;color:#94a3b8"></div>
    </div>
    <div class="grid" id="intraday-aggregates"></div>
  </div>
  </div>

  <div id="view-browse" hidden>
  <h2>Browse Collected Data</h2>
  <div class="card" style="margin-bottom:2rem">
    <form id="browse-range" style="display:flex;gap:.75rem;align-items:flex-end;flex-wrap:wrap;margin-bottom:1rem">
      <div class="tabs" id="browse-mode" style="margin:0">
        <button type="button" class="tab active" data-mode="raw">Raw observations</button>
        <button type="button" class="tab" data-mode="archive">Archived hourly</button>
      </div>
      <label style="font-size:.75rem;color:#94a3b8">From<br>
        <input type="date" name="start" required style="margin-top:.3rem">
      </label>
      <label style="font-size:.75rem;color:#94a3b8">To<br>
        <input type="date" name="end" required style="margin-top:.3rem">
      </label>
      <button type="submit" style="padding:.45rem 1rem;border-radius:.4rem;border:none;background:#7dd3fc;color:#0f172a;font-weight:600;cursor:pointer">Load</button>
      <a id="browse-csv" href="#" style="padding:.45rem 1rem;border-radius:.4rem;background:#334155;color:#e2e8f0;text-decoration:none">Download CSV for this range</a>
    </form>
    <div id="browse-note" class="sub" style="margin-bottom:.5rem"></div>
    <div style="overflow-x:auto">
      <table>
        <thead id="browse-thead"></thead>
        <tbody id="browse-tbody"><tr><td style="color:#64748b">Pick a date range and click Load.</td></tr></tbody>
      </table>
    </div>
  </div>
  </div>
  </section>

<script>
const fmt = v => v != null ? v : '—';
const fmtDt = iso => iso ? new Date(iso).toLocaleString() : '—';
// What period an ETP value is actually about (window_start..window_end) -
// a single date for a value that spans one calendar day (daily-summary-
// refined PWSWeather, Météo France, or a Tempest/Ecowitt row that happens
// to land on midnight), a date range otherwise (a live rolling-24h window
// that hasn't landed on midnight yet).
const periodLabel = (startIso, endIso) => {
  if (!startIso || !endIso) return '—';
  const start = new Date(startIso), end = new Date(endIso);
  const days = (end - start) / 86400000;
  const d = x => x.toLocaleDateString();
  return days <= 1.1 ? d(start) : `${d(start)} – ${d(end)}`;
};

const methodLabel = {
  penman_monteith:   'Penman-Monteith',
  hargreaves_samani: 'Hargreaves-Samani',
  priestley_taylor:  'Priestley-Taylor',
  makkink:           'Makkink',
  turc:              'Turc',
};
const methodColor = {
  penman_monteith:   '#7dd3fc',
  hargreaves_samani: '#f87171',
  priestley_taylor:  '#34d399',
  makkink:           '#fbbf24',
  turc:              '#c084fc',
};

let lastDailyDays = [];
let dailyStart = '';
let dailyEnd = '';
let browseMode = 'raw';
let browseLoadedOnce = false;
let station = null;
const q = (path, extra = '') => `${path}?station=${encodeURIComponent(station)}${extra}`;

async function loadStations() {
  const { stations = [] } = await fetch('/api/stations').then(r => r.json()).catch(() => ({}));
  const names = stations.map(s => s.station_type);
  station = names.includes(location.hash.slice(1)) ? location.hash.slice(1) : names[0];
  const tabs = document.getElementById('tabs');
  tabs.hidden = names.length < 2;
  tabs.innerHTML = names.map(n => `<button class="tab" data-st="${n}">${n}</button>`).join('');
  tabs.onclick = e => {
    if (!e.target.dataset.st) return;
    station = e.target.dataset.st;
    location.hash = station;
    selectStation();
  };
  selectStation();
}

let researchView = 'monitor';
document.getElementById('research-tabs').onclick = e => {
  const view = e.target.dataset.view;
  if (!view) return;
  researchView = view;
  document.querySelectorAll('#research-tabs .tab').forEach(t => t.classList.toggle('active', t.dataset.view === view));
  document.getElementById('view-monitor').hidden = view !== 'monitor';
  document.getElementById('view-daily').hidden = view !== 'daily';
  document.getElementById('view-intraday').hidden = view !== 'intraday';
  document.getElementById('view-browse').hidden = view !== 'browse';
  if (view === 'daily') drawDailyChart(lastDailyDays);
  if (view === 'intraday' && !intradayLoadedOnce) {
    intradayLoadedOnce = true;
    loadIntraday();
  }
  if (view === 'browse' && !browseLoadedOnce) {
    browseLoadedOnce = true;
    const end = new Date();
    const start = new Date(end.getTime() - 2 * 86400_000);  // last 2 days of raw data by default
    document.querySelector('#browse-range [name=start]').value = start.toISOString().slice(0, 10);
    document.querySelector('#browse-range [name=end]').value = end.toISOString().slice(0, 10);
    loadBrowse();
  }
};

// --- Today/Research top-level section switch, day-stepper, and the
// generalized chart renderer shared by the daily chart, Today's single-line
// accumulation curve, and Research's 5-line intraday curve. ---

const SECTION_KEY = 'navSection', PIN_KEY = 'pinToToday';
let section = 'today';

function todayStr() {
  const d = new Date();
  return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
}
function addDays(dateStr, delta) {
  const d = new Date(dateStr + 'T12:00:00');
  d.setDate(d.getDate() + delta);
  return d.toISOString().slice(0, 10);
}
function fmtDayLabel(dateStr) {
  const d = new Date(dateStr + 'T12:00:00');
  const label = d.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
  return dateStr === todayStr() ? `Today, ${label}` : label;
}

function makeDayStepper({ prevBtn, nextBtn, labelEl, initial, onChange }) {
  let current = initial;
  function render() {
    labelEl.textContent = fmtDayLabel(current);
    nextBtn.disabled = current >= todayStr();
  }
  function set(dateStr) {
    current = dateStr;
    render();
    onChange(current);
  }
  prevBtn.onclick = () => set(addDays(current, -1));
  nextBtn.onclick = () => { if (current < todayStr()) set(addDays(current, 1)); };
  render();
  return { get: () => current, set };
}

document.addEventListener('keydown', e => {
  if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
  if (['INPUT', 'TEXTAREA'].includes(document.activeElement?.tagName)) return;
  const delta = e.key === 'ArrowLeft' ? -1 : 1;
  if (section === 'today') {
    if (delta > 0 && todayStepper.get() >= todayStr()) return;
    todayStepper.set(addDays(todayStepper.get(), delta));
  } else if (section === 'research' && researchView === 'intraday') {
    if (delta > 0 && intradayStepper.get() >= todayStr()) return;
    intradayStepper.set(addDays(intradayStepper.get(), delta));
  }
});

function setState(prefix, state) {
  for (const s of ['loading', 'empty', 'error', 'content']) {
    const el = document.getElementById(`${prefix}-${s}`);
    if (el) el.hidden = s !== state;
  }
}

// Single canvas line-chart renderer shared by the daily chart (date x-axis,
// absolute/delta), Today's accumulation curve (hour x-axis, single line, no
// legend) and Research's intraday curve (hour x-axis, 5 lines, legend) -
// replaces three near-identical bespoke drawing functions.
function drawSeriesChart(canvas, legendEl, xLabels, series, opts = {}) {
  const { showLegend = true, yFmt = v => v.toFixed(1), markPartial = false } = opts;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.clientWidth || 600;
  const H = parseInt(canvas.getAttribute('height'), 10) || 260;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  canvas.style.height = H + 'px';
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);
  if (legendEl) legendEl.innerHTML = '';

  const methods = opts.methods || Object.keys(series);
  const allValues = methods.flatMap(m => series[m] || []).filter(v => v != null);
  if (!xLabels.length || allValues.length === 0) {
    ctx.fillStyle = '#64748b';
    ctx.font = '13px system-ui';
    ctx.fillText(opts.emptyText || 'No data yet.', 12, H / 2);
    return;
  }

  const rawMin = Math.min(0, ...allValues);
  const yMin = rawMin < 0 ? rawMin * 1.1 : 0;
  const yMax = Math.max(...allValues, opts.yMin ?? 0.1) * 1.1;
  const pad = { l: 40, r: 12, t: 10, b: 22 };
  const plotW = W - pad.l - pad.r, plotH = H - pad.t - pad.b;
  const xFor = i => pad.l + (xLabels.length === 1 ? plotW / 2 : (i / (xLabels.length - 1)) * plotW);
  const yFor = v => pad.t + plotH - ((v - yMin) / (yMax - yMin)) * plotH;

  ctx.strokeStyle = '#1e293b';
  ctx.fillStyle = '#64748b';
  ctx.font = '11px system-ui';
  ctx.textAlign = 'right';
  const steps = 4;
  for (let i = 0; i <= steps; i++) {
    const v = yMin + (yMax - yMin) / steps * i;
    const y = yFor(v);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    ctx.fillText(yFmt(v), pad.l - 6, y + 3);
  }
  if (opts.zeroLine && yMin < 0) {
    ctx.strokeStyle = '#94a3b8';
    ctx.lineWidth = 1.5;
    ctx.beginPath(); ctx.moveTo(pad.l, yFor(0)); ctx.lineTo(W - pad.r, yFor(0)); ctx.stroke();
  }

  ctx.textAlign = 'center';
  const tickIdx = xLabels.length === 1 ? [0] : [0, Math.floor((xLabels.length - 1) / 2), xLabels.length - 1];
  tickIdx.forEach(i => ctx.fillText(xLabels[i], xFor(i), H - 4));

  for (const method of methods) {
    const values = series[method];
    if (!values) continue;
    const color = (opts.colorFor ? opts.colorFor(method) : methodColor[method]) || '#94a3b8';

    if (opts.bars) {
      // Discrete per-bucket values (e.g. each hour's own ET0) rather than a
      // continuous curve - independent bars instead of a connected line, so
      // a null bucket just leaves a gap rather than a break in a line.
      const barW = Math.max(3, (plotW / xLabels.length) * 0.6);
      const y0 = yFor(0);
      ctx.fillStyle = color;
      values.forEach((v, i) => {
        if (v == null) return;
        const x = xFor(i), y = yFor(v);
        ctx.fillRect(x - barW / 2, Math.min(y, y0), barW, Math.abs(y0 - y));
      });
    } else {
      let lastDefined = -1;
      values.forEach((v, i) => { if (v != null) lastDefined = i; });

      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      let started = false;
      values.forEach((v, i) => {
        if (v == null) { started = false; return; }
        const x = xFor(i), y = yFor(v);
        if (!started) { ctx.moveTo(x, y); started = true; return; }
        if (markPartial && i === lastDefined) {
          ctx.setLineDash([4, 4]);
          ctx.lineTo(x, y);
          ctx.stroke();
          ctx.beginPath();
          ctx.setLineDash([]);
        } else {
          ctx.lineTo(x, y);
        }
      });
      ctx.stroke();
    }

    if (showLegend) {
      const sw = document.createElement('span');
      sw.innerHTML = `<span style="display:inline-block;width:10px;height:10px;border-radius:2px;` +
        `background:${color};margin-right:.4rem;vertical-align:middle"></span>${opts.labelFor ? opts.labelFor(method) : (methodLabel[method] || method)}`;
      legendEl.appendChild(sw);
    }
  }
}

function hoursToSeries(hoursData) {
  const series = { penman_monteith: hoursData.map(h => h.et0_mm) };
  hoursData.forEach((h, i) => {
    for (const [m, v] of Object.entries(h.et0_alt_mm || {})) {
      (series[m] ??= Array(hoursData.length).fill(null))[i] = v;
    }
  });
  return series;
}

// Running total of each hour's own true ET0 (a real floating window from
// local midnight, growing one hour at a time) - not compute_etp_intraday's
// distribution of an already-known daily total by solar share. A missing
// hour (null - too little data that hour) contributes nothing rather than
// breaking the running total.
function cumulativeFromHourly(hoursData) {
  let running = 0;
  return hoursData.map(h => {
    if (h.et0_mm != null) running += h.et0_mm;
    return { hour: h.hour, et0_mm: Math.round(running * 1000) / 1000 };
  });
}

function drawTodayChart(hoursData, finalized) {
  const canvas = document.getElementById('today-chart');
  const xLabels = hoursData.map(h => String(h.hour));
  drawSeriesChart(canvas, null, xLabels, { penman_monteith: hoursData.map(h => h.et0_mm) }, {
    showLegend: false, yMin: 0.1, markPartial: !finalized,
    emptyText: 'No accumulation data for this day.',
    methods: ['penman_monteith'],
  });
}

function drawTodayHourlyChart(hoursData) {
  const canvas = document.getElementById('today-hourly-chart');
  const xLabels = hoursData.map(h => String(h.hour));
  drawSeriesChart(canvas, null, xLabels, { penman_monteith: hoursData.map(h => h.et0_mm) }, {
    showLegend: false, yMin: 0.05, bars: true,
    emptyText: 'No hourly data for this day yet.',
    methods: ['penman_monteith'],
  });
}

function drawIntradayChart(hoursData, finalized) {
  const canvas = document.getElementById('intraday-chart');
  const legendEl = document.getElementById('intraday-legend');
  const xLabels = hoursData.map(h => String(h.hour));
  drawSeriesChart(canvas, legendEl, xLabels, hoursToSeries(hoursData), {
    showLegend: true, yMin: 0.1, markPartial: !finalized,
    emptyText: 'No intraday data for this station/day.',
  });
}

let todayStepper, intradayStepper, intradayLoadedOnce = false;
let lastTodayHours = { hours: [], finalized: true };
let lastTodayHourlyRaw = { hours: [] };
let lastIntradayHours = { hours: [], finalized: true };

async function loadToday() {
  if (!station) return;
  const dateStr = todayStepper.get();
  setState('today', 'loading');
  let summary;
  try {
    summary = await fetch(q('/api/observations/daily-summary', `&date=${dateStr}`)).then(r => r.ok ? r.json() : Promise.reject());
  } catch {
    setState('today', 'error');
    return;
  }
  if (summary.et0_mm == null && !summary.aggregates) {
    setState('today', 'empty');
    return;
  }
  setState('today', 'content');
  const finalized = dateStr !== todayStr();
  document.getElementById('today-badge').hidden = finalized;
  document.getElementById('today-et0').innerHTML = summary.et0_mm != null
    ? `${summary.et0_mm.toFixed(2)}<span class="unit">mm/day</span>`
    : `—<span class="unit">mm/day</span>`;
  const agg = summary.aggregates || {};
  document.getElementById('chip-solar').textContent = agg.solar_radiation_mean != null
    ? `${(agg.solar_radiation_mean * 86400 / 1e6).toFixed(1)} MJ/m²/day` : '—';
  document.getElementById('chip-temp').textContent = agg.temperature_mean != null ? `${agg.temperature_mean.toFixed(1)} °C` : '—';
  document.getElementById('chip-temp-range').textContent = (agg.temperature_min != null && agg.temperature_max != null)
    ? `min ${agg.temperature_min.toFixed(1)} / max ${agg.temperature_max.toFixed(1)}` : '';
  document.getElementById('chip-hum').textContent = agg.humidity_mean != null ? `${agg.humidity_mean.toFixed(0)} %` : '—';
  document.getElementById('chip-dew').textContent = agg.dew_point_mean != null ? `${agg.dew_point_mean.toFixed(1)} °C` : '—';
  document.getElementById('chip-wind').textContent = agg.wind_speed_mean != null ? `${agg.wind_speed_mean.toFixed(1)} m/s` : '—';

  const hourly = await fetch(q('/api/etp/hourly', `&date=${dateStr}`))
    .then(r => r.ok ? r.json() : { hours: [] }).catch(() => ({ hours: [] }));
  // A finalized day shows its full 24h (gaps and all); today-in-progress only
  // shows hours that have actually happened yet - otherwise the trailing,
  // not-yet-occurred hours would flatline the running total out to hour 24
  // and look like the day is already "done".
  const elapsedHour = finalized ? 24 : new Date().getHours() + 1;
  const hoursSoFar = hourly.hours.slice(0, Math.max(0, Math.min(24, elapsedHour)));
  lastTodayHourlyRaw = { hours: hoursSoFar };
  drawTodayHourlyChart(hoursSoFar);
  const cumulative = cumulativeFromHourly(hoursSoFar);
  lastTodayHours = { hours: cumulative, finalized };
  drawTodayChart(cumulative, finalized);
}

async function loadIntraday() {
  if (!station) return;
  const dateStr = intradayStepper.get();
  setState('intraday', 'loading');
  let summary, intraday;
  try {
    [summary, intraday] = await Promise.all([
      fetch(q('/api/observations/daily-summary', `&date=${dateStr}`)).then(r => r.ok ? r.json() : Promise.reject()),
      fetch(q('/api/etp/intraday', `&date=${dateStr}`)).then(r => r.ok ? r.json() : Promise.reject()),
    ]);
  } catch {
    setState('intraday', 'error');
    return;
  }
  if (summary.et0_mm == null && !summary.aggregates) {
    setState('intraday', 'empty');
    return;
  }
  setState('intraday', 'content');
  document.getElementById('intraday-badge').hidden = dateStr !== todayStr();
  lastIntradayHours = intraday;
  drawIntradayChart(intraday.hours, intraday.finalized);
  const agg = summary.aggregates || {};
  const rows = [
    ['Temp min/max/mean', agg.temperature_min, agg.temperature_max, agg.temperature_mean, '°C', 1],
    ['RH min/max/mean', agg.humidity_min, agg.humidity_max, agg.humidity_mean, '%', 0],
    ['Dew point mean', null, null, agg.dew_point_mean, '°C', 1],
    ['Wind mean', null, null, agg.wind_speed_mean, 'm/s', 1],
    ['Solar mean', null, null, agg.solar_radiation_mean, 'W/m²', 0],
  ];
  document.getElementById('intraday-aggregates').innerHTML = rows.map(([label, min, max, mean, unit, dec]) => `
    <div class="card">
      <div class="lbl">${label}</div>
      <div class="val" style="font-size:1.1rem">
        ${min != null ? min.toFixed(dec) + ' / ' : ''}${max != null ? max.toFixed(dec) + ' / ' : ''}${mean != null ? mean.toFixed(dec) : '—'}<span class="unit">${unit}</span>
      </div>
    </div>`).join('');
}

document.getElementById('today-retry').onclick = loadToday;
document.getElementById('intraday-retry').onclick = loadIntraday;

function setSection(newSection, { persist = true } = {}) {
  section = newSection;
  document.querySelectorAll('.section-btn').forEach(b => b.classList.toggle('active', b.dataset.section === section));
  document.getElementById('section-today').hidden = section !== 'today';
  document.getElementById('section-research').hidden = section !== 'research';
  if (persist) localStorage.setItem(SECTION_KEY, section);
  if (section === 'today') loadToday();
}

document.getElementById('section-nav').onclick = e => {
  const s = e.target.dataset.section;
  if (!s) return;
  setSection(s);
};

const pinToggle = document.getElementById('pin-today-toggle');
pinToggle.checked = localStorage.getItem(PIN_KEY) === '1';
pinToggle.onchange = () => localStorage.setItem(PIN_KEY, pinToggle.checked ? '1' : '0');

let dailyMode = 'absolute';
document.getElementById('daily-mode').onclick = e => {
  const mode = e.target.dataset.mode;
  if (!mode || mode === dailyMode) return;
  dailyMode = mode;
  document.querySelectorAll('#daily-mode .tab').forEach(t => t.classList.toggle('active', t.dataset.mode === mode));
  drawDailyChart(lastDailyDays);
};

document.getElementById('daily-range').onsubmit = e => {
  e.preventDefault();
  dailyStart = e.target.elements.start.value;
  dailyEnd = e.target.elements.end.value;
  loadDaily();
};

document.getElementById('daily-range-clear').onclick = () => {
  dailyStart = ''; dailyEnd = '';
  document.querySelector('#daily-range [name=start]').value = '';
  document.querySelector('#daily-range [name=end]').value = '';
  loadDaily();
};

async function loadDaily() {
  if (!station) return;
  let extra = '';
  if (dailyStart) extra += `&start=${dailyStart}`;
  if (dailyEnd) extra += `&end=${dailyEnd}`;
  const daily = await fetch(q('/api/etp/daily', extra)).then(r => r.ok ? r.json() : null).catch(() => null);
  lastDailyDays = daily?.days || [];
  drawDailyChart(lastDailyDays);
}

const fmtNum = decimals => v => v != null ? v.toFixed(decimals) : '—';
const BROWSE_COLUMNS = {
  raw: [
    ['Timestamp', 'timestamp', fmtDt],
    ['Temp °C', 'temperature_c', fmtNum(1)],
    ['Humidity %', 'humidity_pct', fmtNum(0)],
    ['Wind m/s', 'wind_speed_ms', fmtNum(1)],
    ['Solar W/m²', 'solar_radiation_wm2', fmtNum(0)],
    ['Pressure hPa', 'pressure_hpa', fmtNum(1)],
  ],
  archive: [
    ['Hour', 'hour_start', fmtDt],
    ['Temp min', 'temperature_min_c', fmtNum(1)],
    ['Temp max', 'temperature_max_c', fmtNum(1)],
    ['Temp mean', 'temperature_mean_c', fmtNum(1)],
    ['RH min', 'humidity_min_pct', fmtNum(0)],
    ['RH max', 'humidity_max_pct', fmtNum(0)],
    ['RH mean', 'humidity_mean_pct', fmtNum(0)],
    ['Wind min', 'wind_speed_min_ms', fmtNum(1)],
    ['Wind max', 'wind_speed_max_ms', fmtNum(1)],
    ['Wind mean', 'wind_speed_mean_ms', fmtNum(1)],
    ['Solar min', 'solar_radiation_min_wm2', fmtNum(0)],
    ['Solar max', 'solar_radiation_max_wm2', fmtNum(0)],
    ['Solar mean', 'solar_radiation_mean_wm2', fmtNum(0)],
    ['Pressure min', 'pressure_min_hpa', fmtNum(1)],
    ['Pressure max', 'pressure_max_hpa', fmtNum(1)],
    ['Pressure mean', 'pressure_mean_hpa', fmtNum(1)],
    ['Samples', 'sample_count', v => fmt(v)],
  ],
};

document.getElementById('browse-mode').onclick = e => {
  const mode = e.target.dataset.mode;
  if (!mode || mode === browseMode) return;
  browseMode = mode;
  document.querySelectorAll('#browse-mode .tab').forEach(t => t.classList.toggle('active', t.dataset.mode === mode));
  loadBrowse();
};

document.getElementById('browse-range').onsubmit = e => {
  e.preventDefault();
  loadBrowse();
};

async function loadBrowse() {
  if (!station) return;
  const form = document.getElementById('browse-range');
  const start = form.elements.start.value;
  const end = form.elements.end.value;
  const note = document.getElementById('browse-note');
  const cols = BROWSE_COLUMNS[browseMode];

  const endpoint = browseMode === 'archive' ? '/api/observations/archive' : '/api/observations/range';
  const exportEndpoint = browseMode === 'archive' ? '/api/export/archive' : '/api/export/range';
  document.getElementById('browse-csv').href =
    `${exportEndpoint}?station=${encodeURIComponent(station)}&start=${start}&end=${end}`;

  document.getElementById('browse-thead').innerHTML = `<tr>${cols.map(([label]) => `<th>${label}</th>`).join('')}</tr>`;
  const tbody = document.getElementById('browse-tbody');

  if (!start || !end) {
    note.textContent = '';
    tbody.innerHTML = `<tr><td colspan="${cols.length}" style="color:#64748b">Pick a date range and click Load.</td></tr>`;
    return;
  }

  tbody.innerHTML = `<tr><td colspan="${cols.length}" style="color:#64748b">loading…</td></tr>`;
  let data = null, error = null;
  try {
    const r = await fetch(`${endpoint}?station=${encodeURIComponent(station)}&start=${start}&end=${end}`);
    if (r.ok) data = await r.json();
    else error = `HTTP ${r.status}`;
  } catch (err) {
    error = err.message || 'network error';
  }

  if (error) {
    tbody.innerHTML = `<tr><td colspan="${cols.length}" style="color:#f87171">Failed to load data (${error}) — try again.</td></tr>`;
    note.textContent = '';
    return;
  }
  if (data.rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${cols.length}" style="color:#64748b">No data in this range</td></tr>`;
    note.textContent = '';
    return;
  }

  tbody.innerHTML = data.rows.slice().reverse().map(row =>
    `<tr>${cols.map(([, key, format]) => `<td>${format(row[key])}</td>`).join('')}</tr>`
  ).join('');
  note.textContent = data.total > data.rows.length
    ? `Showing the most recent ${data.rows.length} of ${data.total} rows — narrow the date range or download the CSV for everything.`
    : `${data.total} row(s)`;
}

function selectStation() {
  document.querySelectorAll('#tabs .tab').forEach(t => t.classList.toggle('active', t.dataset.st === station));
  document.getElementById('export-daily').href = q('/api/export/daily');
  document.getElementById('export-station').value = station;
  // Clear the previous station's values until the new ones arrive.
  for (const [id, unit] of [['etp','mm/day'],['temp','°C'],['hum','%'],['sol','W/m²'],['wind','m/s']])
    document.getElementById(id).innerHTML = `—<span class="unit">${unit}</span>`;
  document.getElementById('etp-sub').textContent = 'awaiting data…';
  document.getElementById('last-seen').textContent = '';
  document.getElementById('methods').innerHTML = '<tr><td colspan="3" style="color:#64748b">loading…</td></tr>';
  loadDaily();
  if (browseLoadedOnce) loadBrowse();
  if (intradayLoadedOnce) loadIntraday();
  if (section === 'today') loadToday();
  refresh();
}

async function refresh() {
  if (!station) return;
  const [etp, obs, health, hist] = await Promise.all([
    fetch(q('/api/etp/current')).then(r => r.ok ? r.json() : null).catch(() => null),
    fetch(q('/api/observations/latest')).then(r => r.ok ? r.json() : null).catch(() => null),
    fetch(q('/health')).then(r => r.json()).catch(() => ({})),
    fetch(q('/api/etp/history', '&limit=48')).then(r => r.json()).catch(() => ({entries:[]})),
  ]);
  loadDaily();

  if (etp) {
    document.getElementById('etp').innerHTML =
      `${etp.et0_mm_per_day.toFixed(2)}<span class="unit">mm/day</span>`;
    document.getElementById('etp-sub').textContent =
      `For: ${periodLabel(etp.window_start, etp.window_end)}`;

    const rows = [['Penman-Monteith (reference)', etp.et0_mm_per_day, 0]];
    for (const [key, val] of Object.entries(etp.et0_alt_mm_per_day || {})) {
      rows.push([methodLabel[key] || key, val, val - etp.et0_mm_per_day]);
    }
    document.getElementById('methods').innerHTML = rows.map(([label, val, delta]) =>
      `<tr>
        <td>${label}</td>
        <td>${val.toFixed(3)}</td>
        <td>${delta === 0 ? '—' : (delta > 0 ? '+' : '') + delta.toFixed(3)}</td>
      </tr>`
    ).join('');
  }

  if (obs) {
    document.getElementById('temp').innerHTML  = `${fmt(obs.temperature_c?.toFixed(1))}<span class="unit">°C</span>`;
    document.getElementById('hum').innerHTML   = `${fmt(obs.humidity_pct?.toFixed(0))}<span class="unit">%</span>`;
    document.getElementById('sol').innerHTML   = `${fmt(obs.solar_radiation_wm2?.toFixed(0))}<span class="unit">W/m²</span>`;
    document.getElementById('wind').innerHTML  = `${fmt(obs.wind_speed_ms?.toFixed(1))}<span class="unit">m/s</span>`;
  }

  const dot  = document.getElementById('dot');
  const conn = document.getElementById('conn');
  if (health.station_connected) {
    dot.className = 'dot ok'; conn.textContent = 'Connected';
  } else {
    dot.className = 'dot err'; conn.textContent = 'No signal';
  }
  if (health.last_observation_at) {
    document.getElementById('last-seen').textContent =
      `Last: ${fmtDt(health.last_observation_at)}`;
  }
  if (health.observation_retention_days != null) {
    document.getElementById('retention-days').textContent = health.observation_retention_days;
  }

  const tbody = document.getElementById('hist');
  tbody.innerHTML = hist.entries.length === 0
    ? '<tr><td colspan="3" style="color:#64748b">no history yet</td></tr>'
    : hist.entries.map(e =>
        `<tr>
          <td>${periodLabel(e.window_start, e.window_end)}</td>
          <td>${e.et0_mm_per_day.toFixed(3)}</td>
          <td>${fmt(e.observation_count)}</td>
        </tr>`
      ).join('');
}

function drawDailyChart(days) {
  const canvas = document.getElementById('daily-chart');
  const legendEl = document.getElementById('daily-legend');
  if (!days.length) {
    drawSeriesChart(canvas, legendEl, [], {}, { emptyText: 'No daily data yet — check back after the next hourly computation.' });
    return;
  }
  const xLabels = days.map(d => d.date);
  const series = {};
  if (dailyMode === 'absolute') series.penman_monteith = days.map(d => d.et0_mm);
  days.forEach((d, i) => {
    for (const [method, value] of Object.entries(d.et0_alt_mm)) {
      (series[method] ??= Array(days.length).fill(null))[i] = dailyMode === 'delta' ? value - d.et0_mm : value;
    }
  });
  drawSeriesChart(canvas, legendEl, xLabels, series, { zeroLine: dailyMode === 'delta' });
}

window.addEventListener('resize', () => {
  drawDailyChart(lastDailyDays);
  drawTodayChart(lastTodayHours.hours, lastTodayHours.finalized);
  drawTodayHourlyChart(lastTodayHourlyRaw.hours);
  drawIntradayChart(lastIntradayHours.hours, lastIntradayHours.finalized);
});

todayStepper = makeDayStepper({
  prevBtn: document.getElementById('today-prev'),
  nextBtn: document.getElementById('today-next'),
  labelEl: document.getElementById('today-label'),
  initial: addDays(todayStr(), -1),
  onChange: loadToday,
});
intradayStepper = makeDayStepper({
  prevBtn: document.getElementById('intraday-prev'),
  nextBtn: document.getElementById('intraday-next'),
  labelEl: document.getElementById('intraday-label'),
  initial: addDays(todayStr(), -1),
  onChange: loadIntraday,
});
setSection(localStorage.getItem(PIN_KEY) === '1' ? 'today' : (localStorage.getItem(SECTION_KEY) || 'today'), { persist: false });

loadStations();
setInterval(() => {
  refresh();
  if (section === 'today' && todayStepper.get() === todayStr()) loadToday();
  if (researchView === 'intraday' && intradayLoadedOnce && intradayStepper.get() === todayStr()) loadIntraday();
}, 30_000);
</script>
</body>
</html>
"""
