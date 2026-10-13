import logging
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qsl

from .base import Observation, WeatherStationListener

logger = logging.getLogger(__name__)

ECOWITT_HTTP_PORT = 8090

MPH_TO_MS   = 0.44704
INHG_TO_HPA = 33.8639


def _float(params: dict, key: str) -> float | None:
    try:
        return float(params[key])
    except (KeyError, ValueError):
        return None


def _f_to_c(f: float | None) -> float | None:
    return None if f is None else (f - 32.0) * 5.0 / 9.0


def _timestamp(dateutc: str | None) -> int:
    # Gateways send "YYYY-MM-DD HH:MM:SS" (UTC); some firmwares send "now".
    if dateutc:
        try:
            dt = datetime.strptime(dateutc, "%Y-%m-%d %H:%M:%S")
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            pass
    return int(time.time())


class EcowittListener(WeatherStationListener):
    """Ecowitt sensors (WS90, WS80, WH65…) via a gateway/console "Customized"
    upload using the Ecowitt protocol: the gateway HTTP-POSTs form-encoded
    imperial readings to this listener every N seconds.

    Configure in WS View Plus / gateway web UI → Weather Services → Customized:
    protocol Ecowitt, server = this host's IP, port = ECOWITT_PORT, any path,
    interval ≤ 60 s.
    """

    name = "ecowitt"

    def __init__(self, db, port: int | None = None, passkey: str | None = None):
        super().__init__(db)
        self.port = port or int(os.environ.get("ECOWITT_PORT", ECOWITT_HTTP_PORT))
        # Optional: only accept uploads from the gateway with this PASSKEY
        # (MD5 of its MAC, visible in the first upload logged below).
        self.passkey = (passkey or os.environ.get("ECOWITT_PASSKEY") or "").upper() or None
        self._seen_gateways: set[str] = set()

    def _run(self):
        listener = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode(errors="replace")
                listener._handle(dict(parse_qsl(body)))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                pass  # one upload every few seconds; don't spam the log

        server = HTTPServer(("0.0.0.0", self.port), Handler)
        server.timeout = 1.0
        logger.info("Listening for Ecowitt uploads on HTTP port %d", self.port)

        while not self._stop.is_set():
            try:
                server.handle_request()
            except Exception as e:
                logger.error("Listener error: %s", e)
                time.sleep(1)

        server.server_close()

    def _handle(self, params: dict):
        passkey = params.get("PASSKEY", "").upper()
        if passkey not in self._seen_gateways:
            self._seen_gateways.add(passkey)
            logger.info("Ecowitt upload from %s (model=%s, PASSKEY=%s)",
                        params.get("stationtype"), params.get("model"), passkey)
        if self.passkey and passkey != self.passkey:
            return

        wind_mph = _float(params, "windspeedmph")
        baro_in  = _float(params, "baromabsin")   # station (absolute) pressure

        record: Observation = {
            "timestamp":       _timestamp(params.get("dateutc")),
            "temperature":     _f_to_c(_float(params, "tempf")),
            "humidity":        _float(params, "humidity"),
            "wind_speed":      None if wind_mph is None else wind_mph * MPH_TO_MS,
            "solar_radiation": _float(params, "solarradiation"),
            "pressure":        None if baro_in is None else baro_in * INHG_TO_HPA,
        }
        self.publish(record)
