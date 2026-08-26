import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from calculator import compute_etp
from database import Database
from listener import TempestListener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

LATITUDE          = float(os.environ.get("LATITUDE", "48.8566"))
ELEVATION         = float(os.environ.get("ELEVATION", "100"))
ANEMOMETER_HEIGHT = float(os.environ.get("ANEMOMETER_HEIGHT", "2.0"))
DATA_DIR          = os.environ.get("DATA_DIR", "/data")

db       = Database(f"{DATA_DIR}/observations.db")
listener = TempestListener(db)
sched    = BackgroundScheduler()


def run_etp():
    try:
        result = compute_etp(db, LATITUDE, ELEVATION, ANEMOMETER_HEIGHT)
        if result:
            db.store_etp(result)
            logger.info("ETP %.3f mm/day (%d obs)", result["et0_mm"], result["observation_count"])
        else:
            logger.warning("Insufficient observations for ETP computation")
    except Exception:
        logger.exception("ETP computation failed")


def run_cleanup():
    db.cleanup_old_observations(keep_hours=49)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init()
    listener.start()
    sched.add_job(run_etp,     "cron", minute=0)       # top of every hour
    sched.add_job(run_cleanup, "cron", hour=3, minute=0)  # daily purge
    sched.start()
    run_etp()   # immediate first computation on startup
    yield
    sched.shutdown(wait=False)
    listener.stop()


app = FastAPI(title="Tempest ETP", lifespan=lifespan)


def _iso(ts: int | None) -> str | None:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None


@app.get("/health")
def health():
    obs = db.get_latest_observation()
    last_ts = obs["timestamp"] if obs else None
    connected = last_ts is not None and (time.time() - last_ts) < 120
    return {
        "status": "ok",
        "station_connected": connected,
        "last_observation_at": _iso(last_ts),
        "latitude": LATITUDE,
        "elevation_m": ELEVATION,
        "anemometer_height_m": ANEMOMETER_HEIGHT,
    }


@app.get("/api/etp/current")
def current_etp():
    r = db.get_latest_etp()
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
def etp_history(limit: int = 48):
    rows = db.get_etp_history(limit)
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
def latest_obs():
    obs = db.get_latest_observation()
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


@app.get("/", response_class=HTMLResponse)
def index():
    return _HTML


_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Tempest ETP</title>
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
  </style>
</head>
<body>
  <h1>Tempest ETP Monitor</h1>
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

<script>
const fmt = v => v != null ? v : '—';
const fmtDt = iso => iso ? new Date(iso).toLocaleString() : '—';

async function refresh() {
  const [etp, obs, health, hist] = await Promise.all([
    fetch('/api/etp/current').then(r => r.ok ? r.json() : null).catch(() => null),
    fetch('/api/observations/latest').then(r => r.ok ? r.json() : null).catch(() => null),
    fetch('/health').then(r => r.json()).catch(() => ({})),
    fetch('/api/etp/history?limit=48').then(r => r.json()).catch(() => ({entries:[]})),
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

refresh();
setInterval(refresh, 30_000);
</script>
</body>
</html>
"""
