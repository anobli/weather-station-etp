import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

INCOMPLETE_COLUMNS = ("dew_point", "latitude", "elevation", "anemometer_height")


class Database:
    def __init__(self, path: str):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()

    def init(self):
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL UNIQUE,
                    temperature  REAL,
                    humidity     REAL,
                    wind_speed   REAL,
                    solar_radiation REAL,
                    pressure     REAL,
                    dew_point    REAL,
                    latitude     REAL,
                    elevation    REAL,
                    anemometer_height REAL
                );
                CREATE INDEX IF NOT EXISTS idx_obs_ts ON observations(timestamp);

                CREATE TABLE IF NOT EXISTS observations_hourly (
                    hour_start INTEGER PRIMARY KEY,
                    temperature_min REAL, temperature_max REAL, temperature_mean REAL,
                    humidity_min REAL, humidity_max REAL, humidity_mean REAL,
                    wind_speed_min REAL, wind_speed_max REAL, wind_speed_mean REAL,
                    solar_radiation_min REAL, solar_radiation_max REAL, solar_radiation_mean REAL,
                    pressure_min REAL, pressure_max REAL, pressure_mean REAL,
                    dew_point_min REAL, dew_point_max REAL, dew_point_mean REAL,
                    latitude_mean REAL, elevation_mean REAL, anemometer_height_mean REAL,
                    sample_count INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS etp_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    computed_at     INTEGER NOT NULL,
                    window_start    INTEGER NOT NULL,
                    window_end      INTEGER NOT NULL,
                    et0_mm          REAL NOT NULL,
                    observation_count INTEGER,
                    et0_alt_mm      TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_etp_ts ON etp_results(computed_at);
            """)
            # Migration for DBs created before et0_alt_mm existed.
            columns = {row["name"] for row in c.execute("PRAGMA table_info(etp_results)")}
            if "et0_alt_mm" not in columns:
                c.execute("ALTER TABLE etp_results ADD COLUMN et0_alt_mm TEXT")

            # Migration for DBs created before dew_point/latitude/elevation/
            # anemometer_height existed.
            columns = {row["name"] for row in c.execute("PRAGMA table_info(observations)")}
            for col in ("dew_point", "latitude", "elevation", "anemometer_height"):
                if col not in columns:
                    c.execute(f"ALTER TABLE observations ADD COLUMN {col} REAL")

            # Same, for observations_hourly.
            columns = {row["name"] for row in c.execute("PRAGMA table_info(observations_hourly)")}
            for col in ("dew_point_min", "dew_point_max", "dew_point_mean",
                        "latitude_mean", "elevation_mean", "anemometer_height_mean"):
                if col not in columns:
                    c.execute(f"ALTER TABLE observations_hourly ADD COLUMN {col} REAL")

            # One row per window_end: the normal hourly compute_etp cron
            # never *intends* two rows for the same wall-clock window_end,
            # but restarting more than once within the same observation
            # minute (e.g. during a redeploy) could already have produced
            # duplicates via the old plain INSERT before this index existed -
            # dedupe (keep the highest id, i.e. most recently stored) before
            # adding the index, or its creation fails on exactly that data.
            indexes = {row["name"] for row in c.execute("PRAGMA index_list(etp_results)")}
            if "idx_etp_window_end" not in indexes:
                c.execute(
                    "DELETE FROM etp_results WHERE id NOT IN "
                    "(SELECT MAX(id) FROM etp_results GROUP BY window_end)"
                )
                c.execute("CREATE UNIQUE INDEX idx_etp_window_end ON etp_results(window_end)")

    def insert_observation(self, obs: dict):
        with self._conn() as c:
            c.execute(
                """INSERT OR REPLACE INTO observations
                   (timestamp, temperature, humidity, wind_speed, solar_radiation, pressure, dew_point,
                    latitude, elevation, anemometer_height)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (obs["timestamp"], obs["temperature"], obs["humidity"],
                 obs["wind_speed"], obs["solar_radiation"], obs["pressure"], obs.get("dew_point"),
                 obs.get("latitude"), obs.get("elevation"), obs.get("anemometer_height")),
            )

    def get_window(self, hours: int = 24) -> list[dict]:
        cutoff = int(time.time()) - hours * 3600
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM observations WHERE timestamp >= ? ORDER BY timestamp",
                (cutoff,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_latest_observation(self) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM observations ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def store_etp(self, result: dict):
        """Upsert keyed by window_end (see idx_etp_window_end): the normal
        hourly cron always produces a fresh window_end so this behaves like
        a plain insert for it, but a station that republishes an externally
        -published per-day value (stations/meteofrance.py) can safely
        re-store the same day on every refresh without growing duplicates."""
        with self._conn() as c:
            c.execute(
                """INSERT INTO etp_results
                   (computed_at, window_start, window_end, et0_mm, observation_count, et0_alt_mm)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(window_end) DO UPDATE SET
                       computed_at=excluded.computed_at, window_start=excluded.window_start,
                       et0_mm=excluded.et0_mm, observation_count=excluded.observation_count,
                       et0_alt_mm=excluded.et0_alt_mm""",
                (result["computed_at"], result["window_start"], result["window_end"],
                 result["et0_mm"], result["observation_count"],
                 json.dumps(result.get("et0_alt_mm")) if result.get("et0_alt_mm") else None),
            )

    @staticmethod
    def _decode_etp(row: dict) -> dict:
        row["et0_alt_mm"] = json.loads(row["et0_alt_mm"]) if row.get("et0_alt_mm") else {}
        return row

    def get_latest_etp(self) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM etp_results ORDER BY window_end DESC LIMIT 1"
            ).fetchone()
        return self._decode_etp(dict(row)) if row else None

    def get_etp_history(self, limit: int = 48) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM etp_results ORDER BY window_end DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._decode_etp(dict(r)) for r in rows]

    def get_etp_range(self, start_ts: int | None = None, end_ts: int | None = None) -> list[dict]:
        """ETP results (never purged) whose window_end falls between start_ts
        (inclusive) and end_ts (exclusive); unbounded on either side when
        None. Oldest first. Filtered by window_end (the period a row is
        *about*), not computed_at (when it was learned) - those are nearly
        identical for the normal hourly cron, but can diverge a lot for a
        station that republishes historical days in one batch (e.g.
        stations/meteofrance.py backfilling a whole year on each refresh)."""
        query, params = "SELECT * FROM etp_results WHERE 1=1", []
        if start_ts is not None:
            query += " AND window_end >= ?"
            params.append(start_ts)
        if end_ts is not None:
            query += " AND window_end < ?"
            params.append(end_ts)
        query += " ORDER BY window_end ASC"
        with self._conn() as c:
            rows = c.execute(query, params).fetchall()
        return [self._decode_etp(dict(r)) for r in rows]

    def get_range(self, start_ts: int, end_ts: int) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM observations WHERE timestamp >= ? AND timestamp < ? ORDER BY timestamp",
                (start_ts, end_ts),
            ).fetchall()
        return [dict(r) for r in rows]

    def earliest_observation_day(self) -> date | None:
        """Earliest day with data in either table - raw or already archived
        into hourly buckets."""
        with self._conn() as c:
            row = c.execute(
                """SELECT MIN(t) AS t FROM (
                     SELECT MIN(timestamp) AS t FROM observations
                     UNION ALL
                     SELECT MIN(hour_start) AS t FROM observations_hourly
                   )"""
            ).fetchone()
        return None if row["t"] is None else datetime.fromtimestamp(row["t"], tz=timezone.utc).date()

    def incomplete_days(self, start_day: date, end_day: date) -> list[date]:
        """UTC calendar days in [start_day, end_day] (inclusive) that have no
        data at all (in either table), or have at least one row missing a
        column in INCOMPLETE_COLUMNS - e.g. rows recorded before dew_point/
        latitude/elevation/anemometer_height were captured, including ones
        already downsampled into observations_hourly (checked via the
        matching *_mean column). Lets a backfill re-fetch exactly what's
        missing instead of a whole range, however far back it's archived."""
        start_ts = int(datetime(start_day.year, start_day.month, start_day.day, tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime(end_day.year, end_day.month, end_day.day, tzinfo=timezone.utc).timestamp()) + 86400
        raw_incomplete_expr = " OR ".join(f"{col} IS NULL" for col in INCOMPLETE_COLUMNS)
        hourly_incomplete_expr = " OR ".join(f"{col}_mean IS NULL" for col in INCOMPLETE_COLUMNS)
        with self._conn() as c:
            raw_rows = c.execute(
                f"""SELECT (timestamp / 86400) AS day_bucket, COUNT(*) AS cnt,
                           SUM(CASE WHEN {raw_incomplete_expr} THEN 1 ELSE 0 END) AS incomplete_cnt
                    FROM observations
                    WHERE timestamp >= ? AND timestamp < ?
                    GROUP BY day_bucket""",
                (start_ts, end_ts),
            ).fetchall()
            hourly_rows = c.execute(
                f"""SELECT (hour_start / 86400) AS day_bucket, COUNT(*) AS cnt,
                           SUM(CASE WHEN {hourly_incomplete_expr} THEN 1 ELSE 0 END) AS incomplete_cnt
                    FROM observations_hourly
                    WHERE hour_start >= ? AND hour_start < ?
                    GROUP BY day_bucket""",
                (start_ts, end_ts),
            ).fetchall()
        raw_by_bucket = {r["day_bucket"]: r for r in raw_rows}
        hourly_by_bucket = {r["day_bucket"]: r for r in hourly_rows}
        epoch = date(1970, 1, 1)
        days = []
        for i in range((end_day - start_day).days + 1):
            d = start_day + timedelta(days=i)
            idx = (d - epoch).days
            raw = raw_by_bucket.get(idx)
            if raw is not None and raw["cnt"] > 0 and raw["incomplete_cnt"] == 0:
                continue  # fully covered by complete raw data
            hourly = hourly_by_bucket.get(idx)
            if hourly is not None and hourly["cnt"] > 0 and hourly["incomplete_cnt"] == 0:
                continue  # already archived, and complete
            days.append(d)
        return days

    def get_archive_range(self, start_ts: int, end_ts: int) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM observations_hourly WHERE hour_start >= ? AND hour_start < ? ORDER BY hour_start",
                (start_ts, end_ts),
            ).fetchall()
        return [dict(r) for r in rows]

    def archive_old_observations(self, keep_hours: int) -> int:
        """Downsample raw observations older than `keep_hours` into hourly
        min/max/mean buckets (observations_hourly, kept indefinitely — a
        single row per hour is negligible storage), then delete the
        now-redundant raw rows. Returns the number of hourly buckets written
        or refreshed."""
        cutoff = int(time.time()) - keep_hours * 3600
        with self._conn() as c:
            cur = c.execute(
                """INSERT OR REPLACE INTO observations_hourly
                   (hour_start, temperature_min, temperature_max, temperature_mean,
                    humidity_min, humidity_max, humidity_mean,
                    wind_speed_min, wind_speed_max, wind_speed_mean,
                    solar_radiation_min, solar_radiation_max, solar_radiation_mean,
                    pressure_min, pressure_max, pressure_mean,
                    dew_point_min, dew_point_max, dew_point_mean,
                    latitude_mean, elevation_mean, anemometer_height_mean, sample_count)
                   SELECT (timestamp / 3600) * 3600,
                          MIN(temperature), MAX(temperature), AVG(temperature),
                          MIN(humidity), MAX(humidity), AVG(humidity),
                          MIN(wind_speed), MAX(wind_speed), AVG(wind_speed),
                          MIN(solar_radiation), MAX(solar_radiation), AVG(solar_radiation),
                          MIN(pressure), MAX(pressure), AVG(pressure),
                          MIN(dew_point), MAX(dew_point), AVG(dew_point),
                          AVG(latitude), AVG(elevation), AVG(anemometer_height),
                          COUNT(*)
                   FROM observations
                   WHERE timestamp < ?
                   GROUP BY (timestamp / 3600) * 3600""",
                (cutoff,),
            )
            written = cur.rowcount
            c.execute("DELETE FROM observations WHERE timestamp < ?", (cutoff,))
        return written
