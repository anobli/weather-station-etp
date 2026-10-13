from .base import Observation, WeatherStationListener
from .ecowitt import EcowittListener
from .tempest import TempestListener

# STATION_TYPE value -> driver class. Register new drivers here.
DRIVERS: dict[str, type[WeatherStationListener]] = {
    TempestListener.name: TempestListener,
    EcowittListener.name: EcowittListener,
}


def create_listener(station_type: str, db) -> WeatherStationListener:
    try:
        cls = DRIVERS[station_type.lower()]
    except KeyError:
        raise ValueError(
            f"Unknown STATION_TYPE={station_type!r}; available: {', '.join(sorted(DRIVERS))}"
        ) from None
    return cls(db)


__all__ = ["Observation", "WeatherStationListener", "TempestListener", "EcowittListener", "DRIVERS", "create_listener"]
