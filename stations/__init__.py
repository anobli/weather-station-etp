from .base import Observation, WeatherStationListener
from .ecowitt import EcowittListener
from .meteofrance import MeteoFranceListener
from .pwsweather import PWSWeatherListener
from .tempest import TempestListener

# STATION_TYPE value -> driver class. Register new drivers here.
DRIVERS: dict[str, type[WeatherStationListener]] = {
    TempestListener.name: TempestListener,
    EcowittListener.name: EcowittListener,
    PWSWeatherListener.name: PWSWeatherListener,
    MeteoFranceListener.name: MeteoFranceListener,
}


def create_listener(station_type: str, db, instance_id: str | None = None) -> WeatherStationListener:
    try:
        cls = DRIVERS[station_type.lower()]
    except KeyError:
        raise ValueError(
            f"Unknown STATION_TYPE={station_type!r}; available: {', '.join(sorted(DRIVERS))}"
        ) from None
    return cls(db, instance_id=instance_id)


__all__ = [
    "Observation", "WeatherStationListener", "TempestListener", "EcowittListener",
    "PWSWeatherListener", "MeteoFranceListener", "DRIVERS", "create_listener",
]
