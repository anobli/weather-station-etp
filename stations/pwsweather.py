import json
import logging
import os
import urllib.error
import urllib.request
from urllib.parse import quote, urlencode

from .base import Observation, WeatherStationListener

logger = logging.getLogger(__name__)

# PWSWeather station data is served through Xweather (formerly AerisWeather),
# the API provider behind the PWSWeather network. Requires a developer
# account: https://www.xweather.com/ -> client_id/client_secret.
API_BASE = "https://data.api.xweather.com/observations"
POLL_SECONDS_DEFAULT = 300


class PWSWeatherListener(WeatherStationListener):
    """A single PWSWeather personal weather station, polled via the Xweather API.

    Unlike Tempest/Ecowitt, a PWSWeather station isn't reachable on the local
    network: readings are pulled from Xweather's cloud API on a timer rather
    than received as they arrive, so freshness is bounded by `poll_seconds`.
    """

    name = "pwsweather"

    def __init__(self, db, station_id: str | None = None, client_id: str | None = None,
                 client_secret: str | None = None, poll_seconds: int | None = None):
        super().__init__(db)
        self.station_id = station_id or os.environ.get("PWSWEATHER_STATION_ID")
        self.client_id = client_id or os.environ.get("PWSWEATHER_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("PWSWEATHER_CLIENT_SECRET")
        self.poll_seconds = poll_seconds or int(os.environ.get("PWSWEATHER_POLL_SECONDS", POLL_SECONDS_DEFAULT))
        self.stale_after = self.poll_seconds * 2 + 60
        if not self.station_id:
            raise ValueError("PWSWEATHER_STATION_ID is required for the pwsweather driver")
        if not self.client_id or not self.client_secret:
            raise ValueError("PWSWEATHER_CLIENT_ID/PWSWEATHER_CLIENT_SECRET are required for the pwsweather driver")
        self._last_timestamp: int | None = None

    def _run(self):
        logger.info("Polling PWSWeather station %s every %ds", self.station_id, self.poll_seconds)
        while not self._stop.is_set():
            try:
                self._poll()
            except Exception as e:
                logger.error("PWSWeather poll error: %s", e)
            if self._stop.wait(self.poll_seconds):
                break

    def _poll(self):
        url = f"{API_BASE}/pws:{quote(self.station_id)}?" + urlencode({
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                payload = json.load(resp)
        except urllib.error.URLError as e:
            logger.error("PWSWeather request failed: %s", e)
            return

        if not payload.get("success"):
            logger.error("PWSWeather API error: %s", payload.get("error"))
            return

        ob = (payload.get("response") or {}).get("ob") or {}
        timestamp = ob.get("timestamp")
        if timestamp is None or timestamp == self._last_timestamp:
            return  # nothing new since the last poll
        self._last_timestamp = timestamp

        record: Observation = {
            "timestamp":       int(timestamp),
            "temperature":     ob.get("tempC"),
            "humidity":        ob.get("humidity"),
            "wind_speed":      ob.get("windSpeedMPS"),
            "solar_radiation": ob.get("solradWM2"),
            "pressure":        ob.get("spressureMB", ob.get("pressureMB")),
        }
        self.publish(record)
