import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import TypedDict

logger = logging.getLogger(__name__)


class Observation(TypedDict):
    """Normalised observation every station driver must produce (SI units)."""
    timestamp:       int           # epoch seconds UTC
    temperature:     float         # air temperature (°C)
    humidity:        float         # relative humidity (%)
    wind_speed:      float | None  # wind average (m/s)
    solar_radiation: float | None  # global solar radiation (W/m²)
    pressure:        float | None  # station pressure (hPa)
    dew_point:       float | None  # dew point (°C), when the station reports it directly
    # Location/instrument config as in effect when this row was recorded,
    # carried on every row (not just configured once elsewhere) so a CSV
    # export alone has everything needed to recompute ET0.
    latitude:          float | None  # site latitude (decimal degrees)
    elevation:         float | None  # site elevation (m)
    anemometer_height: float | None  # wind sensor height (m)


class WeatherStationListener(ABC):
    """Base class for weather station drivers.

    Owns the background thread and DB persistence; a driver only implements
    `_run()`, which loops until `self._stop` is set and calls `publish()`
    for each observation it decodes.
    """

    name: str = "station"
    # False for a driver whose ETP shouldn't come from main.py's hourly
    # rolling-24h-from-raw-observations pass (run_etp/calculator.compute_etp):
    # either it republishes an externally-published value directly
    # (stations/meteofrance.py - no raw observations to compute anything
    # from), or it has its own, better per-day source and would rather
    # have ETP only reflect a complete calendar day than an always-live
    # but less accurate rolling estimate (stations/pwsweather.py, refined
    # from Xweather's own daily summary - see main.py's
    # run_pwsweather_summary_refresh).
    computes_own_etp: bool = True
    # False for a driver with no raw per-reading observations to compute a
    # true single-hour ET0 from (main.py's run_etp hourly pass + startup
    # backfill, feeding Today's hourly/accumulated charts). Separate from
    # computes_own_etp: PWSWeather takes its *daily* ETP from Xweather's
    # summary but still polls raw readings every few minutes, which is
    # plenty for the hourly value.
    computes_hourly_etp: bool = True
    # Seconds since the last observation after which a station is considered
    # disconnected; override for drivers that poll on a longer interval.
    stale_after: float = 120

    def __init__(self, db, instance_id: str | None = None):
        self.db = db
        # Vendor-specific identifier distinguishing this instance from other
        # stations of the same type (e.g. a hub serial number, gateway
        # PASSKEY, or station id); None for a single, unnamed instance.
        self.instance_id = instance_id
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_seen: float | None = None
        # Drivers whose backing API reports the station's own registered
        # location (e.g. PWSWeather/Xweather) fill these in once learned, so
        # ETP uses the real station coordinates instead of the LATITUDE/
        # ELEVATION env vars, which are a single value shared across every
        # configured station and easy to leave at a wrong/default value.
        self.latitude: float | None = None
        self.elevation: float | None = None
        # Longitude isn't needed by the FAO-56 ET0 formula itself (daily Ra
        # only depends on latitude + day-of-year), so it's not part of
        # Observation/publish() - it only exists for looking up the nearest
        # Météo France ETPgrille comparison point (meteofrance.py).
        self.longitude: float | None = None
        # Fallback location/instrument config for drivers that never learn
        # their own from an API (Tempest, Ecowitt) - main.py overwrites
        # these from the LATITUDE/LONGITUDE/ELEVATION/ANEMOMETER_HEIGHT env
        # vars right after construction. publish() stamps every row with
        # whichever of latitude/elevation/anemometer_height is in effect.
        self.default_latitude: float = 48.8566
        self.default_longitude: float = 2.3522
        self.default_elevation: float = 100.0
        self.anemometer_height: float = 2.0
        # Mirrors main.py's OBSERVATION_RETENTION_HOURS, so a driver that
        # backfills old data (past retention) can immediately fold it into
        # the hourly archive itself, instead of leaving it in the raw table
        # until the next scheduled archive run.
        self.observation_retention_hours: int = 8760

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"{self.name}-listener")
        self._thread.start()
        logger.info("%s listener started%s", self.name, f" (instance={self.instance_id})" if self.instance_id else "")

    def stop(self):
        self._stop.set()

    @abstractmethod
    def _run(self):
        """Blocking receive loop; must return promptly once `self._stop` is set."""

    def publish(self, record: Observation):
        if record.get("temperature") is None or record.get("humidity") is None:
            return
        record = dict(record)
        record.setdefault("latitude", self.latitude if self.latitude is not None else self.default_latitude)
        record.setdefault("elevation", self.elevation if self.elevation is not None else self.default_elevation)
        record.setdefault("anemometer_height", self.anemometer_height)
        try:
            self.db.insert_observation(record)
            self.last_seen = time.time()
        except Exception as e:
            logger.error("DB insert error: %s", e)
