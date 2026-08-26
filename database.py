import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path


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
                    pressure     REAL
                );
                CREATE INDEX IF NOT EXISTS idx_obs_ts ON observations(timestamp);

                CREATE TABLE IF NOT EXISTS etp_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    computed_at     INTEGER NOT NULL,
                    window_start    INTEGER NOT NULL,
                    window_end      INTEGER NOT NULL,
                    et0_mm          REAL NOT NULL,
                    observation_count INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_etp_ts ON etp_results(computed_at);
            """)

    def insert_observation(self, obs: dict):
        with self._conn() as c:
            c.execute(
                """INSERT OR REPLACE INTO observations
                   (timestamp, temperature, humidity, wind_speed, solar_radiation, pressure)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (obs["timestamp"], obs["temperature"], obs["humidity"],
                 obs["wind_speed"], obs["solar_radiation"], obs["pressure"]),
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
        with self._conn() as c:
            c.execute(
                """INSERT INTO etp_results
                   (computed_at, window_start, window_end, et0_mm, observation_count)
                   VALUES (?, ?, ?, ?, ?)""",
                (result["computed_at"], result["window_start"], result["window_end"],
                 result["et0_mm"], result["observation_count"]),
            )

    def get_latest_etp(self) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM etp_results ORDER BY computed_at DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def get_etp_history(self, limit: int = 48) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM etp_results ORDER BY computed_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def cleanup_old_observations(self, keep_hours: int = 49):
        cutoff = int(time.time()) - keep_hours * 3600
        with self._conn() as c:
            c.execute("DELETE FROM observations WHERE timestamp < ?", (cutoff,))
