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
# Comma-separated list, e.g. "tempest,ecowitt,pwsweather"; the first one is the API default.
STATION_TYPES     = list(dict.fromkeys(
    s.strip().lower() for s in os.environ.get("STATION_TYPE", "tempest").split(",") if s.strip()
))

try:
    TZ = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Paris"))
except ZoneInfoNotFoundError:
    logger.warning("Unknown TIMEZONE=%r, falling back to UTC", os.environ.get("TIMEZONE"))
    TZ = ZoneInfo("UTC")



def _db_path(station_type: str) -> str:
    return f"{DATA_DIR}/observations-{station_type}.db"


# Pre-multi-station deployments used a single observations.db: hand it to the
# first configured station so its ETP history is kept.
_legacy_db = f"{DATA_DIR}/observations.db"
if os.path.exists(_legacy_db) and not os.path.exists(_db_path(STATION_TYPES[0])):
    logger.info("Migrating %s to %s", _legacy_db, _db_path(STATION_TYPES[0]))
    os.rename(_legacy_db, _db_path(STATION_TYPES[0]))

# One database (observations + ETP) per station.
stations: dict[str, WeatherStationListener] = {
    t: create_listener(t, Database(_db_path(t))) for t in STATION_TYPES
}
sched = BackgroundScheduler()


def _station(name: str | None) -> WeatherStationListener:
    if name is None:
        return stations[STATION_TYPES[0]]
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
                logger.info("[%s] ETP %.3f mm/day (%d obs)",
                            name, result["et0_mm"], result["observation_count"])
            else:
                logger.warning("[%s] Insufficient observations for ETP computation", name)
        except Exception:
            logger.exception("[%s] ETP computation failed", name)


def run_cleanup():
    for st in stations.values():
        st.db.cleanup_old_observations(keep_hours=49)


@asynccontextmanager
async def lifespan(app: FastAPI):
    for st in stations.values():
        st.db.init()
        st.start()
    sched.add_job(run_etp,     "cron", minute=0)       # top of every hour
    sched.add_job(run_cleanup, "cron", hour=3, minute=0)  # daily purge
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
                "observation_count": r["observation_count"],
            }
            for r in rows
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


def _csv_response(rows: list[dict], filename: str) -> Response:
    return Response(
        content=export.rows_to_csv(rows, TZ),
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
      <button type="submit" style="padding:.45rem 1rem;border-radius:.4rem;border:none;background:#7dd3fc;color:#0f172a;font-weight:600;cursor:pointer">Download CSV</button>
    </form>
  </div>

<script>
const fmt = v => v != null ? v : '—';
const fmtDt = iso => iso ? new Date(iso).toLocaleString() : '—';

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

function selectStation() {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.st === station));
  document.getElementById('export-daily').href = q('/api/export/daily');
  document.getElementById('export-station').value = station;
  // Clear the previous station's values until the new ones arrive.
  for (const [id, unit] of [['etp','mm/day'],['temp','°C'],['hum','%'],['sol','W/m²'],['wind','m/s']])
    document.getElementById(id).innerHTML = `—<span class="unit">${unit}</span>`;
  document.getElementById('etp-sub').textContent = 'awaiting data…';
  document.getElementById('last-seen').textContent = '';
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

  if (etp) {
    document.getElementById('etp').innerHTML =
      `${etp.et0_mm_per_day.toFixed(2)}<span class="unit">mm/day</span>`;
    document.getElementById('etp-sub').textContent =
      `Computed: ${fmtDt(etp.computed_at)}`;
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

loadStations();
setInterval(refresh, 30_000);
</script>
</body>
</html>
"""
