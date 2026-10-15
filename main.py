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
from calculator import compute_etp
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
        try:
            result = compute_etp(st.db, LATITUDE, ELEVATION, ANEMOMETER_HEIGHT)
            if result:
                st.db.store_etp(result)
                alt = ", ".join(f"{m}={v:.3f}" for m, v in result["et0_alt_mm"].items())
                logger.info("[%s] ETP penman_monteith=%.3f mm/day (%d obs) %s",
                            name, result["et0_mm"], result["observation_count"], alt)
            else:
                logger.warning("[%s] Insufficient observations for ETP computation", name)
        except Exception:
            logger.exception("[%s] ETP computation failed", name)


def run_archive():
    for name, st in stations.items():
        written = st.db.archive_old_observations(keep_hours=OBSERVATION_RETENTION_HOURS)
        if written:
            logger.info("[%s] Archived %d hourly bucket(s) of raw observations", name, written)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for st in stations.values():
        st.db.init()
        st.start()
    sched.add_job(run_etp,     "cron", minute=0)       # top of every hour
    sched.add_job(run_archive, "cron", hour=3, minute=0)  # daily downsample + purge
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
    last_ts = obs["timestamp"] if obs else None
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
    return {
        "status": "ok",
        "commit": GIT_SHA,
        **_station_status(_station(station)),
        "latitude": LATITUDE,
        "elevation_m": ELEVATION,
        "anemometer_height_m": ANEMOMETER_HEIGHT,
        "observation_retention_days": round(OBSERVATION_RETENTION_HOURS / 24, 1),
    }


@app.get("/api/etp/current")
def current_etp(station: str | None = None):
    r = _station(station).db.get_latest_etp()
    if not r:
        return JSONResponse(status_code=503, content={"error": "No ETP data yet"})
    return {
        "computed_at":      _iso(r["computed_at"]),
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
                "computed_at":      _iso(r["computed_at"]),
                "et0_mm_per_day":   r["et0_mm"],
                "et0_alt_mm_per_day": r["et0_alt_mm"],
                "observation_count": r["observation_count"],
            }
            for r in rows
        ]
    }


@app.get("/api/etp/daily")
def etp_daily(station: str | None = None, start: date | None = None, end: date | None = None):
    """One point per local calendar day, averaging that day's hourly rolling-24h
    computations (etp_results is never purged, so this covers the station's
    whole history, not just the last 49h of raw observations). `start`/`end`
    (local calendar dates, inclusive) optionally restrict the range."""
    if start and end and end < start:
        raise HTTPException(status_code=400, detail="end date must be on or after start date")
    start_ts = export.local_day_bounds(start, TZ)[0] if start else None
    end_ts = export.local_day_bounds(end, TZ)[1] if end else None
    rows = _station(station).db.get_etp_range(start_ts, end_ts)

    days: dict[str, dict] = {}
    for r in rows:
        day = datetime.fromtimestamp(r["computed_at"], tz=TZ).date().isoformat()
        bucket = days.setdefault(day, {"et0_mm": [], "et0_alt_mm": {}})
        bucket["et0_mm"].append(r["et0_mm"])
        for method, value in r["et0_alt_mm"].items():
            bucket["et0_alt_mm"].setdefault(method, []).append(value)

    return {
        "days": [
            {
                "date":    day,
                "et0_mm":  round(sum(b["et0_mm"]) / len(b["et0_mm"]), 3),
                "et0_alt_mm": {
                    method: round(sum(values) / len(values), 3)
                    for method, values in b["et0_alt_mm"].items()
                },
                "sample_count": len(b["et0_mm"]),
            }
            for day, b in sorted(days.items())
        ]
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
    if end < start:
        raise HTTPException(status_code=400, detail="end date must be on or after start date")
    start_ts, _ = export.local_day_bounds(start, TZ)
    _, end_ts = export.local_day_bounds(end, TZ)
    rows = st.db.get_range(start_ts, end_ts)
    return _csv_response(rows, f"weather-station-{st.name}-{start.isoformat()}_to_{end.isoformat()}.csv")


@app.get("/api/export/archive")
def export_archive(start: date, end: date, station: str | None = None):
    """CSV of archived hourly min/max/mean buckets (beyond OBSERVATION_RETENTION_HOURS,
    raw per-minute data is downsampled into these rather than deleted)."""
    st = _station(station)
    if end < start:
        raise HTTPException(status_code=400, detail="end date must be on or after start date")
    start_ts, _ = export.local_day_bounds(start, TZ)
    _, end_ts = export.local_day_bounds(end, TZ)
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
  </style>
</head>
<body>
  <h1>Weather Station ETP Monitor</h1>
  <nav class="tabs" id="tabs" hidden></nav>
  <nav class="tabs" id="view-tabs">
    <button class="tab active" data-view="overview">Overview</button>
    <button class="tab" data-view="chart">Daily ETP Chart</button>
  </nav>

  <div id="view-overview">
  <div class="grid">
    <div class="card">
      <div class="lbl">ETP — rolling 24 h</div>
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

  <h2>ETP History — last 48 computations</h2>
  <table>
    <thead><tr><th>Computed at (UTC)</th><th>ETP mm/day</th><th>Observations</th></tr></thead>
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
  </div>

  <div id="view-chart" hidden>
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
    <canvas id="daily-chart" height="260" style="width:100%;max-width:100%"></canvas>
    <div id="daily-legend" style="display:flex;gap:1rem;flex-wrap:wrap;margin-top:.75rem;font-size:.75rem;color:#94a3b8"></div>
    <div class="sub" style="margin-top:.5rem">Each point averages that day's hourly rolling-24h computations.</div>
  </div>
  </div>

<script>
const fmt = v => v != null ? v : '—';
const fmtDt = iso => iso ? new Date(iso).toLocaleString() : '—';

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

document.getElementById('view-tabs').onclick = e => {
  const view = e.target.dataset.view;
  if (!view) return;
  document.querySelectorAll('#view-tabs .tab').forEach(t => t.classList.toggle('active', t.dataset.view === view));
  document.getElementById('view-overview').hidden = view !== 'overview';
  document.getElementById('view-chart').hidden = view !== 'chart';
  if (view === 'chart') drawDailyChart(lastDailyDays);
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
      `Computed: ${fmtDt(etp.computed_at)}`;

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
          <td>${fmtDt(e.computed_at)}</td>
          <td>${e.et0_mm_per_day.toFixed(3)}</td>
          <td>${e.observation_count}</td>
        </tr>`
      ).join('');
}

function drawDailyChart(days) {
  const canvas = document.getElementById('daily-chart');
  const legendEl = document.getElementById('daily-legend');
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.clientWidth || 600;
  const H = 260;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  canvas.style.height = H + 'px';
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W, H);
  legendEl.innerHTML = '';

  if (!days.length) {
    ctx.fillStyle = '#64748b';
    ctx.font = '13px system-ui';
    ctx.fillText('No daily data yet — check back after the next hourly computation.', 12, H / 2);
    return;
  }

  const series = { penman_monteith: days.map(d => d.et0_mm) };
  days.forEach((d, i) => {
    for (const [method, value] of Object.entries(d.et0_alt_mm)) {
      (series[method] ??= Array(days.length).fill(null))[i] = value;
    }
  });

  const allValues = Object.values(series).flat().filter(v => v != null);
  const yMax = Math.max(...allValues, 0.1) * 1.1;
  const pad = { l: 36, r: 12, t: 10, b: 22 };
  const plotW = W - pad.l - pad.r;
  const plotH = H - pad.t - pad.b;
  const xFor = i => pad.l + (days.length === 1 ? plotW / 2 : (i / (days.length - 1)) * plotW);
  const yFor = v => pad.t + plotH - (v / yMax) * plotH;

  ctx.strokeStyle = '#1e293b';
  ctx.fillStyle = '#64748b';
  ctx.font = '11px system-ui';
  ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i++) {
    const v = (yMax / 4) * i;
    const y = yFor(v);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    ctx.fillText(v.toFixed(1), pad.l - 6, y + 3);
  }

  ctx.textAlign = 'center';
  [0, Math.floor((days.length - 1) / 2), days.length - 1].forEach(i => {
    ctx.fillText(days[i].date, xFor(i), H - 4);
  });

  for (const [method, values] of Object.entries(series)) {
    const color = methodColor[method] || '#94a3b8';
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    let started = false;
    values.forEach((v, i) => {
      if (v == null) { started = false; return; }
      const x = xFor(i), y = yFor(v);
      if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); }
    });
    ctx.stroke();

    const sw = document.createElement('span');
    sw.innerHTML = `<span style="display:inline-block;width:10px;height:10px;border-radius:2px;` +
      `background:${color};margin-right:.4rem;vertical-align:middle"></span>${methodLabel[method] || method}`;
    legendEl.appendChild(sw);
  }
}

window.addEventListener('resize', () => drawDailyChart(lastDailyDays));

loadStations();
setInterval(refresh, 30_000);
</script>
</body>
</html>
"""
