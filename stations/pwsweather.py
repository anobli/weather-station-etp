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
# account: https://www.xweather.com/ -> client_id/client_secret, issued by
# self-serve trial signups as a single "API key" shaped CLIENTID_SECRET.
API_BASE = "https://data.api.xweather.com/observations"
POLL_SECONDS_DEFAULT = 300


def _split_api_key(key: str) -> tuple[str, str]:
    client_id, _, client_secret = key.partition("_")
    if not client_id or not client_secret:
        raise ValueError("PWSWEATHER_API_KEY must be in CLIENTID_CLIENTSECRET form")
    return client_id, client_secret


class PWSWeatherListener(WeatherStationListener):
    """A single PWSWeather personal weather station, polled via the Xweather API.

    Unlike Tempest/Ecowitt, a PWSWeather station isn't reachable on the local
    network: readings are pulled from Xweather's cloud API on a timer rather
    than received as they arrive, so freshness is bounded by `poll_seconds`.
    Unlike those drivers, several instances of this one can run
    concurrently (it only ever makes outbound HTTP calls, so there's no
    shared port to conflict over): `instance_id` (e.g. from
    "pwsweather:HALOLVANTAGE") is used directly as the station id, taking
    priority over PWSWEATHER_STATION_ID, letting several PWSWeather
    stations be configured without separate env vars per instance - they'd
    typically still share one PWSWEATHER_API_KEY/_CLIENT_ID/_SECRET (same
    Xweather account).
    """

    name = "pwsweather"

    def __init__(self, db, station_id: str | None = None, client_id: str | None = None,
                 client_secret: str | None = None, poll_seconds: int | None = None,
                 instance_id: str | None = None):
        super().__init__(db, instance_id=instance_id)
        station_id = instance_id or station_id or os.environ.get("PWSWEATHER_STATION_ID")
        # Xweather expects PWSWeather station ids prefixed with "PWS_"
        # (e.g. PWS_KAZMTPHX); add it if the configured id doesn't have it.
        self.station_id = (
            station_id if station_id is None or station_id.upper().startswith("PWS_")
            else f"PWS_{station_id}"
        )
        self.client_id = client_id or os.environ.get("PWSWEATHER_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("PWSWEATHER_CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            api_key = os.environ.get("PWSWEATHER_API_KEY")
            if api_key:
                self.client_id, self.client_secret = _split_api_key(api_key)
        self.poll_seconds = poll_seconds or int(os.environ.get("PWSWEATHER_POLL_SECONDS", POLL_SECONDS_DEFAULT))
        self.stale_after = self.poll_seconds * 2 + 60
        if not self.station_id:
            raise ValueError("PWSWEATHER_STATION_ID is required for the pwsweather driver")
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "pwsweather driver needs either PWSWEATHER_API_KEY (CLIENTID_SECRET form) "
                "or PWSWEATHER_CLIENT_ID + PWSWEATHER_CLIENT_SECRET"
            )
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
        url = f"{API_BASE}/{quote(self.station_id)}?" + urlencode({
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

        response = payload.get("response") or {}
        ob = response.get("ob") or {}
        timestamp = ob.get("timestamp")
        if timestamp is None or timestamp == self._last_timestamp:
            return  # nothing new since the last poll
        self._last_timestamp = timestamp

        if self.latitude is None or self.elevation is None:
            loc = response.get("loc") or {}
            profile = response.get("profile") or {}
            lat, elev = loc.get("lat"), profile.get("elevM")
            if lat is not None and elev is not None:
                self.latitude, self.elevation = lat, elev
                logger.info(
                    "PWSWeather station %s registered at lat=%s elevation=%sm (overrides LATITUDE/ELEVATION env)",
                    self.station_id, lat, elev,
                )

        record: Observation = {
            "timestamp":       int(timestamp),
            "temperature":     ob.get("tempC"),
            "humidity":        ob.get("humidity"),
            "wind_speed":      ob.get("windSpeedMPS"),
            "solar_radiation": ob.get("solradWM2"),
            "pressure":        ob.get("spressureMB", ob.get("pressureMB")),
            "dew_point":       ob.get("dewpointC"),
        }
        self.publish(record)
