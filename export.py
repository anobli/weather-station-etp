import csv
import io
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

COLUMNS = [
    ("timestamp_utc",       lambda r, tz: datetime.fromtimestamp(r["timestamp"], tz=timezone.utc).isoformat()),
    ("timestamp_local",     lambda r, tz: datetime.fromtimestamp(r["timestamp"], tz=tz).isoformat()),
    ("temperature_c",       lambda r, tz: r["temperature"]),
    ("humidity_pct",        lambda r, tz: r["humidity"]),
    ("wind_speed_ms",       lambda r, tz: r["wind_speed"]),
    ("solar_radiation_wm2", lambda r, tz: r["solar_radiation"]),
    ("pressure_hpa",        lambda r, tz: r["pressure"]),
    ("dew_point_c",         lambda r, tz: r["dew_point"]),
    # Location/instrument config in effect when the row was recorded - carried
    # per row (not just configured once elsewhere) so this CSV alone has
    # everything ET0 needs to be recomputed later.
    ("latitude_deg",            lambda r, tz: r["latitude"]),
    ("elevation_m",             lambda r, tz: r["elevation"]),
    ("anemometer_height_m",     lambda r, tz: r["anemometer_height"]),
]

# Downsampled hourly buckets (observations_hourly), used once raw
# observations age past OBSERVATION_RETENTION_HOURS and get archived.
ARCHIVE_COLUMNS = [
    ("hour_start_utc",   lambda r, tz: datetime.fromtimestamp(r["hour_start"], tz=timezone.utc).isoformat()),
    ("hour_start_local", lambda r, tz: datetime.fromtimestamp(r["hour_start"], tz=tz).isoformat()),
    ("temperature_min_c",       lambda r, tz: r["temperature_min"]),
    ("temperature_max_c",       lambda r, tz: r["temperature_max"]),
    ("temperature_mean_c",      lambda r, tz: r["temperature_mean"]),
    ("humidity_min_pct",        lambda r, tz: r["humidity_min"]),
    ("humidity_max_pct",        lambda r, tz: r["humidity_max"]),
    ("humidity_mean_pct",       lambda r, tz: r["humidity_mean"]),
    ("wind_speed_min_ms",       lambda r, tz: r["wind_speed_min"]),
    ("wind_speed_max_ms",       lambda r, tz: r["wind_speed_max"]),
    ("wind_speed_mean_ms",      lambda r, tz: r["wind_speed_mean"]),
    ("solar_radiation_min_wm2", lambda r, tz: r["solar_radiation_min"]),
    ("solar_radiation_max_wm2", lambda r, tz: r["solar_radiation_max"]),
    ("solar_radiation_mean_wm2", lambda r, tz: r["solar_radiation_mean"]),
    ("pressure_min_hpa",        lambda r, tz: r["pressure_min"]),
    ("pressure_max_hpa",        lambda r, tz: r["pressure_max"]),
    ("pressure_mean_hpa",       lambda r, tz: r["pressure_mean"]),
    ("dew_point_min_c",         lambda r, tz: r["dew_point_min"]),
    ("dew_point_max_c",         lambda r, tz: r["dew_point_max"]),
    ("dew_point_mean_c",        lambda r, tz: r["dew_point_mean"]),
    ("latitude_deg",            lambda r, tz: r["latitude_mean"]),
    ("elevation_m",             lambda r, tz: r["elevation_mean"]),
    ("anemometer_height_m",     lambda r, tz: r["anemometer_height_mean"]),
    ("sample_count",            lambda r, tz: r["sample_count"]),
]


def local_day_bounds(day: date, tz: ZoneInfo) -> tuple[int, int]:
    """Return (start_ts, end_ts) epoch seconds spanning local midnight `day` to midnight `day + 1`."""
    start = datetime.combine(day, time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


def rows_to_csv(rows: list[dict], tz: ZoneInfo, columns=COLUMNS) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([name for name, _ in columns])
    for r in rows:
        writer.writerow([fn(r, tz) for _, fn in columns])
    return buf.getvalue()
