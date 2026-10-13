import json
import logging
import socket
import time

from .base import Observation, WeatherStationListener

logger = logging.getLogger(__name__)

TEMPEST_UDP_PORT = 50222


class TempestListener(WeatherStationListener):
    """WeatherFlow Tempest, via the hub's local UDP broadcast API."""

    name = "tempest"

    def __init__(self, db, port: int = TEMPEST_UDP_PORT):
        super().__init__(db)
        self.port = port

    def _run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("0.0.0.0", self.port))
        sock.settimeout(1.0)
        logger.info("Listening for Tempest UDP broadcasts on port %d", self.port)

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
            record: Observation = {
                "timestamp":       obs[0],   # epoch seconds UTC
                "wind_speed":      obs[2],   # wind avg (m/s)
                "pressure":        obs[6],   # station pressure (hPa/MB)
                "temperature":     obs[7],   # air temperature (°C)
                "humidity":        obs[8],   # relative humidity (%)
                "solar_radiation": obs[11],  # solar radiation (W/m²)
            }
            self.publish(record)
