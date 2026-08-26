import json
import logging
import socket
import threading
import time

logger = logging.getLogger(__name__)

TEMPEST_UDP_PORT = 50222


class TempestListener:
    def __init__(self, db):
        self.db = db
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_seen: float | None = None

    def start(self):
        self._thread = threading.Thread(target=self._listen, daemon=True, name="tempest-udp")
        self._thread.start()
        logger.info("Tempest UDP listener started on port %d", TEMPEST_UDP_PORT)

    def stop(self):
        self._stop.set()

    def _listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("0.0.0.0", TEMPEST_UDP_PORT))
        sock.settimeout(1.0)

        while not self._stop.is_set():
            try:
                data, _ = sock.recvfrom(4096)
                msg = json.loads(data.decode())
                self._handle(msg)
            except socket.timeout:
                continue
            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.error("Listener error: %s", e)
                time.sleep(1)

        sock.close()

    def _handle(self, msg: dict):
        # obs_st: full observation from the Tempest sensor (~1 min interval)
        if msg.get("type") != "obs_st":
            return

        for obs in msg.get("obs", []):
            if len(obs) < 17:
                continue

            # WeatherFlow obs_st array layout (indices per API docs)
            record = {
                "timestamp":       obs[0],   # epoch seconds UTC
                "wind_speed":      obs[2],   # wind avg (m/s)
                "pressure":        obs[6],   # station pressure (hPa/MB)
                "temperature":     obs[7],   # air temperature (°C)
                "humidity":        obs[8],   # relative humidity (%)
                "solar_radiation": obs[11],  # solar radiation (W/m²)
            }

            if record["temperature"] is None or record["humidity"] is None:
                continue

            try:
                self.db.insert_observation(record)
                self.last_seen = time.time()
            except Exception as e:
                logger.error("DB insert error: %s", e)
