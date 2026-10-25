import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from urllib.parse import quote, urlencode

from .base import Observation, WeatherStationListener

logger = logging.getLogger(__name__)

# PWSWeather station data is served through Xweather (formerly AerisWeather),
# the API provider behind the PWSWeather network. Requires a developer
# account: https://www.xweather.com/ -> client_id/client_secret, issued by
# self-serve trial signups as a single "API key" shaped CLIENTID_SECRET.
API_BASE = "https://data.api.xweather.com/observations"
# Historical data (2011-08-02 to now); returns one full station-local day
# per call regardless of time-of-day in `from`, and has no `to` parameter.
ARCHIVE_API_BASE = "https://data.api.xweather.com/observations/archive"
# Xweather's own QC'd daily aggregate (max/min/avg temp, RH, wind, dewpoint,
# solar radiation, ...) for the station - same history range as the archive
# endpoint, updated roughly every 20 minutes. Used to refine a day's ETP
# with more authoritative inputs than our own locally-polled samples once
# the day is complete (see main.py's run_pwsweather_summary_refresh).
SUMMARY_API_BASE = "https://data.api.xweather.com/observations/summary"
POLL_SECONDS_DEFAULT = 300
# How many days of history to keep backfilled from the archive endpoint
# (checked on every start, so raising it later pulls in older days too) -
# so ET0 and the dashboard have useful data immediately instead of waiting
# for live polling to accumulate it one poll at a time. 0 disables it.
BACKFILL_DAYS_DEFAULT = 30


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
    # ETP comes from Xweather's own daily summary (run_pwsweather_summary_
    # refresh), not main.py's generic hourly rolling-24h-from-raw-
    # observations pass - a trailing window built from samples polled
    # every PWSWEATHER_POLL_SECONDS is both less accurate than Xweather's
    # full-resolution QC'd aggregate and confusing to show alongside it
    # (two different "ETP" numbers with different meanings for the same
    # station). Raw observations are still polled/stored for the live
    # current-conditions cards and CSV export - just not used for ETP.
    computes_own_etp = False

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
        self.backfill_days = int(os.environ.get("PWSWEATHER_BACKFILL_DAYS", BACKFILL_DAYS_DEFAULT))
        if not self.station_id:
            raise ValueError("PWSWEATHER_STATION_ID is required for the pwsweather driver")
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "pwsweather driver needs either PWSWEATHER_API_KEY (CLIENTID_SECRET form) "
                "or PWSWEATHER_CLIENT_ID + PWSWEATHER_CLIENT_SECRET"
            )
        self._last_timestamp: int | None = None

    def _run(self):
        if self.backfill_days > 0:
            end = date.today()
            earliest = self.db.earliest_observation_day()
            # Cover at least the configured window (so raising
            # PWSWEATHER_BACKFILL_DAYS reaches further back on the next
            # start), plus any older local data, scanning it all for gaps
            # (downtime) or rows missing a field added after they were
            # recorded (dew_point, latitude, ...) - re-running this after
            # adding a new field backfills it everywhere without needing to
            # wipe the DB.
            start = end - timedelta(days=self.backfill_days - 1)
            if earliest is not None:
                start = min(start, earliest)
            gap_days = self.db.incomplete_days(start, end)
            if gap_days:
                logger.info(
                    "%s: backfilling %d incomplete/missing day(s) from the archive (%s to %s)",
                    self.station_id, len(gap_days), gap_days[0], gap_days[-1],
                )
                self.backfill(gap_days)
                # Backfilled days older than retention would otherwise sit
                # in the raw table until the next scheduled archive run;
                # fold them into the hourly archive right away.
                archived = self.db.archive_old_observations(keep_hours=self.observation_retention_hours)
                if archived:
                    logger.info("%s: folded %d backfilled day(s) into the hourly archive", self.station_id, archived)

        logger.info("Polling PWSWeather station %s every %ds", self.station_id, self.poll_seconds)
        while not self._stop.is_set():
            try:
                self._poll()
            except Exception as e:
                logger.error("PWSWeather poll error: %s", e)
            if self._stop.wait(self.poll_seconds):
                break

    def _record_from_ob(self, ob: dict) -> Observation:
        return {
            "timestamp":       int(ob["timestamp"]),
            "temperature":     ob.get("tempC"),
            "humidity":        ob.get("humidity"),
            "wind_speed":      ob.get("windSpeedMPS"),
            "solar_radiation": ob.get("solradWM2"),
            "pressure":        ob.get("spressureMB", ob.get("pressureMB")),
            "dew_point":       ob.get("dewpointC"),
        }

    def _learn_location(self, response: dict):
        if self.latitude is not None and self.elevation is not None:
            return
        loc = response.get("loc") or {}
        profile = response.get("profile") or {}
        lat, lon, elev = loc.get("lat"), loc.get("long"), profile.get("elevM")
        if lat is not None and elev is not None:
            self.latitude, self.longitude, self.elevation = lat, lon, elev
            logger.info(
                "PWSWeather station %s registered at lat=%s lon=%s elevation=%sm (overrides LATITUDE/LONGITUDE/ELEVATION env)",
                self.station_id, lat, lon, elev,
            )

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

        self._learn_location(response)
        self.publish(self._record_from_ob(ob))

    def backfill(self, days: list[date]):
        """Fetch and store every observation for each day in `days`
        (station-local calendar days) from Xweather's archive endpoint - one
        request per day, since that's all it returns per call. Safe to
        re-run: insert_observation is INSERT OR REPLACE keyed by timestamp,
        so re-fetching a day just overwrites it with the same (or more
        complete) data.
        """
        for i, day in enumerate(days):
            url = f"{ARCHIVE_API_BASE}/{quote(self.station_id)}?" + urlencode({
                "from": day.isoformat(),
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            })
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    payload = json.load(resp)
            except urllib.error.URLError as e:
                logger.error("PWSWeather archive request failed for %s: %s", day, e)
                continue
            finally:
                if i < len(days) - 1:
                    time.sleep(1.0)  # politeness between daily archive requests

            if not payload.get("success"):
                logger.error("PWSWeather archive API error for %s: %s", day, payload.get("error"))
                continue

            response = payload.get("response") or {}
            self._learn_location(response)
            inserted = 0
            for period in response.get("periods") or []:
                ob = period.get("ob") or {}
                if ob.get("timestamp") is None:
                    continue
                self.publish(self._record_from_ob(ob))
                inserted += 1
            logger.info("[backfill %s] %s: inserted %d observation(s)", self.station_id, day, inserted)

    def fetch_daily_summary(self, day: date) -> dict | None:
        """Xweather's own daily-aggregate summary for `day` (station-local),
        or None if unavailable (not yet published, request failed, or no
        data for that day). See SUMMARY_API_BASE."""
        url = f"{SUMMARY_API_BASE}/{quote(self.station_id)}?" + urlencode({
            "from": day.isoformat(),
            "to": day.isoformat(),
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                payload = json.load(resp)
        except urllib.error.URLError as e:
            logger.error("PWSWeather summary request failed for %s: %s", day, e)
            return None

        if not payload.get("success"):
            logger.error("PWSWeather summary API error for %s: %s", day, payload.get("error"))
            return None

        periods = ((payload.get("response") or {}).get("periods")) or []
        return periods[0].get("summary") if periods else None
