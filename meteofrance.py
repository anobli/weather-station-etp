"""
Météo France ETPgrille: daily ET0 (mm) interpolated onto a 0.125° grid over
metropolitan France, for comparison against our own station-based ET0.
https://explore.data.gouv.fr/fr/datasets/661d079b6357625e12013241/

Comparison-only, not a computation input: this is a different product
entirely (ARPEGE/AROME model fields interpolated onto the grid, not derived
from local station observations), so divergence from our own FAO-56 number
is expected - more so in areas with strong local relief/microclimate than
on flatter terrain, where the grid point should track closer to reality.

The per-year file is ~4 MB compressed/~115 MB uncompressed (4 147 grid
points x ~365 days); fetched and scanned in full on each refresh rather
than cached on disk, since it's small enough and this only runs once a day.
"""
import csv
import gzip
import io
import logging
import urllib.request
from datetime import date

logger = logging.getLogger(__name__)

# Resolved from the data.gouv.fr dataset API resource list (dataset
# 661d079b6357625e12013241) rather than guessed - note this is NOT the
# object.files.data.gouv.fr/.../QUOT_ETPgrille_latest-{year} pattern some
# other tooling uses, which was found to be several months stale.
URL_TEMPLATE = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/REF_CC/ETP_QUOT/QUOT_ETPgrille_{year}.csv.gz"


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
    logger.info(
        "Météo France grid point (%.3f, %.3f) nearest to (%.4f, %.4f): %d day(s) for %d",
        glon, glat, lat, lon, len(series), year,
    )
    return grid_point, series
