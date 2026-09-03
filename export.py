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
]


def local_day_bounds(day: date, tz: ZoneInfo) -> tuple[int, int]:
    """Return (start_ts, end_ts) epoch seconds spanning local midnight `day` to midnight `day + 1`."""
    start = datetime.combine(day, time.min, tzinfo=tz)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


def rows_to_csv(rows: list[dict], tz: ZoneInfo) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([name for name, _ in COLUMNS])
    for r in rows:
        writer.writerow([fn(r, tz) for _, fn in COLUMNS])
    return buf.getvalue()
