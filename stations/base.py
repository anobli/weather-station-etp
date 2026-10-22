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


class WeatherStationListener(ABC):
    """Base class for weather station drivers.

    Owns the background thread and DB persistence; a driver only implements
    `_run()`, which loops until `self._stop` is set and calls `publish()`
    for each observation it decodes.
    """

    name: str = "station"
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
        try:
            self.db.insert_observation(record)
            self.last_seen = time.time()
        except Exception as e:
            logger.error("DB insert error: %s", e)
