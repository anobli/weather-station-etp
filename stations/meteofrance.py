"""
Météo France ETPgrille as a station: instead of raw sensor observations
fed through our own FAO-56 computation, this republishes Météo France's
own daily ET0 directly into etp_results - the same table every other
station's computed ET0 lands in - so it's addressable through the exact
same API/dashboard as a Tempest or PWSWeather station, just backed by a
public dataset instead of a local sensor.
https://explore.data.gouv.fr/fr/datasets/661d079b6357625e12013241/

There are no raw observations for this station type (nothing to show on
the dashboard's temperature/humidity/wind/solar cards) - that's expected:
this exists for sites where there isn't enough locally-measured data to
run our own calculation, and Météo France's own published number is good
enough on its own.

Known limitation: Météo France's own technical spec documents this product
as inverse-distance-squared interpolation from the 5 nearest real stations,
with NO elevation correction - a plausible source of real divergence from
a local sensor in terrain with strong local relief (confirmed against our
own station + a competitor's in the Alpes-Maritimes: Météo France read
~2-3x higher on at least one day). Flat terrain should fare better.
"""
import csv
import gzip
import io
import logging
import time
import urllib.request
from datetime import date, datetime, timezone

from .base import WeatherStationListener

logger = logging.getLogger(__name__)

URL_TEMPLATE = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/REF_CC/ETP_QUOT/QUOT_ETPgrille_{year}.csv.gz"
# Refreshed once a day, not tied to Météo France's own (observed, not
# documented) ~08:00 UTC publish time - simpler, and self-corrects within a
# day or two either way since this never computes anything itself, just
# republishes whatever the grid currently says for each day.
REFRESH_SECONDS = 24 * 3600


def _download(year: int) -> bytes:
    with urllib.request.urlopen(URL_TEMPLATE.format(year=year), timeout=120) as resp:
        return resp.read()


def _rows(compressed: bytes):
    with gzip.open(io.BytesIO(compressed)) as gz:
        yield from csv.DictReader(io.TextIOWrapper(gz, encoding="utf-8"), delimiter=";")


def _nearest_grid_point(compressed: bytes, lat: float, lon: float) -> tuple[float, float]:
    best, best_dist = None, None
    for row in _rows(compressed):
        glon, glat = float(row["longitude"]), float(row["latitude"])
        d = (glon - lon) ** 2 + (glat - lat) ** 2
        if best_dist is None or d < best_dist:
            best_dist, best = d, (glon, glat)
    return best


def fetch_year(lat: float, lon: float, year: int) -> tuple[tuple[float, float], dict[date, float]]:
    """Return ((grid_lon, grid_lat), {date: etp_mm}) for the grid point
    nearest (lat, lon), for every day of `year` published so far. Two
    passes over the file: one to find the nearest grid point (needs to see
    every unique point), one to collect just that point's series."""
    compressed = _download(year)
    grid_point = _nearest_grid_point(compressed, lat, lon)
    glon, glat = grid_point
    series = {}
    for row in _rows(compressed):
        if float(row["longitude"]) == glon and float(row["latitude"]) == glat:
            d = row["date"]
            series[date(int(d[:4]), int(d[4:6]), int(d[6:8]))] = float(row["ETP_grille"])
    return grid_point, series


class MeteoFranceListener(WeatherStationListener):
    name = "meteofrance"
    computes_own_etp = False
    computes_hourly_etp = False  # daily grid values only, no raw readings
    stale_after = REFRESH_SECONDS * 2 + 3600

    def __init__(self, db, instance_id: str | None = None):
        super().__init__(db, instance_id=instance_id)
        self.grid_point: tuple[float, float] | None = None

    def _run(self):
        while not self._stop.is_set():
            try:
                self._refresh()
            except Exception:
                logger.exception("Météo France refresh failed")
            if self._stop.wait(REFRESH_SECONDS):
                break

    def _refresh(self):
        lat = self.latitude if self.latitude is not None else self.default_latitude
        lon = self.longitude if self.longitude is not None else self.default_longitude
        today = date.today()
        # Early January: this year's file won't have much yet, so also
        # pull last year's tail so the daily chart doesn't suddenly go
        # sparse right after New Year's.
        years = [today.year] if today.month > 1 else [today.year - 1, today.year]

        stored = 0
        for year in years:
            grid_point, series = fetch_year(lat, lon, year)
            self.grid_point = grid_point
            for day, etp in series.items():
                window_start = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
                self.db.store_etp({
                    "computed_at":       int(time.time()),
                    "window_start":      window_start,
                    "window_end":        window_start + 86400,
                    "et0_mm":            etp,
                    "observation_count": None,
                    "et0_alt_mm":        {},
                })
                stored += 1

        self.last_seen = time.time()
        logger.info("[%s] Météo France grid %s: refreshed %d day(s)", self.name, self.grid_point, stored)
