"""
Reference evapotranspiration (ET0, daily, mm/day), computed by several
methods over the same rolling 24-hour window so they can be compared:

- Penman-Monteith (FAO-56): the primary/most accurate method, combining
  radiation, humidity and wind. Reference: Allen et al., FAO Irrigation
  and Drainage Paper No. 56 (1998).
- Hargreaves-Samani (1985): temperature + extraterrestrial radiation only,
  no wind/humidity/solar sensor needed — the usual fallback when those
  aren't available or trusted.
- Priestley-Taylor (1972): net radiation driven, drops the aerodynamic
  (wind) term entirely.
- Makkink (1957): solar radiation + temperature only, simpler than
  Priestley-Taylor since it skips net longwave radiation.
- Turc (1961): solar radiation + temperature, with a dryness correction
  from mean relative humidity. Widely used in French agronomy.
"""

import math
import time
from datetime import date, datetime


def _sat_vp(T: float) -> float:
    """Saturation vapour pressure (kPa) at temperature T (°C)."""
    return 0.6108 * math.exp(17.27 * T / (T + 237.3))


def _compute_from_observations(obs: list[dict], latitude: float, elevation: float,
                                anemometer_height: float, J: int, min_obs: int = 60) -> dict | None:
    """Aggregate a batch of raw observations into the daily inputs FAO-56 needs,
    then run _fao56. Shared by compute_etp (rolling 24h window) and
    compute_etp_intraday (growing same-day prefix) so both paths run the exact
    same formula. Returns None when there are too few observations to be
    meaningful (< min_obs rows with temperature+humidity)."""
    obs = [o for o in obs if o["temperature"] is not None and o["humidity"] is not None]
    if len(obs) < min_obs:
        return None

    temps   = [o["temperature"] for o in obs]
    rhs     = [o["humidity"] for o in obs]
    winds   = [o["wind_speed"] for o in obs if o["wind_speed"] is not None]
    solars  = [o["solar_radiation"] for o in obs if o["solar_radiation"] is not None]
    dews    = [o["dew_point"] for o in obs if o.get("dew_point") is not None]

    # Prefer the latitude/elevation/anemometer_height recorded on each row
    # (in effect when it was observed) over the caller's current config, so
    # a CSV export of `obs` alone is enough to recompute this exact result
    # later even if the live config has since changed.
    lats  = [o["latitude"] for o in obs if o.get("latitude") is not None]
    elevs = [o["elevation"] for o in obs if o.get("elevation") is not None]
    anems = [o["anemometer_height"] for o in obs if o.get("anemometer_height") is not None]
    if lats:
        latitude = sum(lats) / len(lats)
    if elevs:
        elevation = sum(elevs) / len(elevs)
    if anems:
        anemometer_height = sum(anems) / len(anems)

    T_mean = sum(temps) / len(temps)
    T_max  = max(temps)
    T_min  = min(temps)
    RH_mean = sum(rhs) / len(rhs)
    u_mean  = sum(winds) / len(winds) if winds else 0.0
    Rs_wm2  = sum(solars) / len(solars) if solars else 0.0
    T_dew_mean = sum(dews) / len(dews) if dews else None

    result = _fao56(T_mean, T_max, T_min, RH_mean, u_mean, T_dew_mean, Rs_wm2,
                     latitude, elevation, anemometer_height, J)
    timestamps = [o["timestamp"] for o in obs]
    return {
        "window_start":      min(timestamps),
        "window_end":        max(timestamps),
        "observation_count": len(obs),
        **result,
    }


def compute_etp(db, latitude: float, elevation: float, anemometer_height: float = 2.0) -> dict | None:
    """
    Return a result dict {computed_at, window_start, window_end, et0_mm, observation_count}
    or None when there are too few observations to be meaningful (< 6 h of data).
    """
    obs = db.get_window(hours=24)
    timestamps = [o["timestamp"] for o in obs if o["temperature"] is not None and o["humidity"] is not None]
    if not timestamps:
        return None

    # Day of year from midpoint of observation window
    J = datetime.utcfromtimestamp((max(timestamps) + min(timestamps)) / 2).timetuple().tm_yday

    result = _compute_from_observations(obs, latitude, elevation, anemometer_height, J)
    if result is None:
        return None
    return {"computed_at": int(time.time()), **result}


def compute_etp_intraday(obs: list[dict], day_start_ts: int, targets: dict, max_hour: int = 24) -> list[dict]:
    """Cumulative ET0 "through hour h" for a single local calendar day, for
    each method, distributing `targets` (that day's finalized et0_mm/
    et0_alt_mm, or - for today-in-progress - the latest rolling-24h estimate)
    across the hours proportionally to each hour's own mean solar radiation.

    Rerunning the full FAO-56 formula on a growing same-day prefix was tried
    first and rejected: Rn/VPD/wind swing enough hour to hour that the
    "result so far" is not monotonic (it rises *and falls* through the day
    instead of accumulating) - it answers "what would today's rate look like
    with only this much data" rather than "how much ET0 has happened so far".
    Weighting by solar share instead guarantees a curve that rises from 0 to
    exactly `targets` by construction (Rn, the dominant driver of ET0, tracks
    solar input directly) while still reflecting each day's own actual diurnal
    shape. An hour with no observations at all gets weight 0 (flat segment,
    not a guess); if the whole window has no solar data (no sensor, or every
    reading None), falls back to a uniform elapsed-time distribution rather
    than producing an all-zero curve.

    Returns [] if `obs` is empty (no raw/per-minute data for this day at all -
    e.g. a daily-summary-only station) or `targets` has no et0_mm (nothing to
    distribute)."""
    if not obs or targets.get("et0_mm") is None:
        return []

    hourly_solar = [[] for _ in range(max_hour)]
    hourly_has_obs = [False] * max_hour
    for o in obs:
        h = int((o["timestamp"] - day_start_ts) // 3600)
        if not (0 <= h < max_hour):
            continue
        hourly_has_obs[h] = True
        if o.get("solar_radiation") is not None:
            hourly_solar[h].append(o["solar_radiation"])

    weights = [sum(v) / len(v) if v else 0.0 for v in hourly_solar]
    weights = [w if has else 0.0 for w, has in zip(weights, hourly_has_obs)]
    total_weight = sum(weights)
    if total_weight <= 0:
        # No usable solar signal - spread evenly over hours that have any data.
        weights = [1.0 if has else 0.0 for has in hourly_has_obs]
        total_weight = sum(weights)
    if total_weight <= 0:
        return []

    alt_methods = list(targets.get("et0_alt_mm") or {})
    hours, running = [], 0.0
    for h in range(max_hour):
        running += weights[h]
        frac = running / total_weight
        hours.append({
            "hour": h + 1,
            "et0_mm": round(targets["et0_mm"] * frac, 3),
            "et0_alt_mm": {m: round(targets["et0_alt_mm"][m] * frac, 3) for m in alt_methods},
        })
    return hours


def compute_etp_from_summary(summary: dict, latitude: float, elevation: float,
                              anemometer_height: float, day: date) -> dict | None:
    """Like compute_etp, but from a pre-aggregated daily summary (e.g. Xweather's
    /observations/summary) instead of deriving min/max/mean from locally-polled
    samples - a vendor's own QC'd daily aggregate reflects their full data
    stream for the station, not just whatever we managed to poll every
    PWSWEATHER_POLL_SECONDS. `summary` is that endpoint's per-day object
    (temp/rh/wind/dewpt/solrad sub-objects, each with maxC/minC/avgC or
    max/min/avg depending on the field). Returns None if the minimum inputs
    (temp max/min/avg, rh avg) aren't present."""
    temp = summary.get("temp") or {}
    rh = summary.get("rh") or {}
    wind = summary.get("wind") or {}
    dewpt = summary.get("dewpt") or {}
    solrad = summary.get("solrad") or {}

    T_max, T_min, T_mean, RH_mean = temp.get("maxC"), temp.get("minC"), temp.get("avgC"), rh.get("avg")
    if T_max is None or T_min is None or T_mean is None or RH_mean is None:
        return None

    J = day.timetuple().tm_yday
    result = _fao56(T_mean, T_max, T_min, RH_mean, wind.get("avgMPS") or 0.0, dewpt.get("avgC"),
                     solrad.get("avgWM2") or 0.0, latitude, elevation, anemometer_height, J)
    return {
        "computed_at":       int(time.time()),
        "observation_count": temp.get("count"),
        **result,
    }


def _fao56(T_mean: float, T_max: float, T_min: float, RH_mean: float, u_mean: float,
           T_dew_mean: float | None, Rs_wm2: float, latitude: float, elevation: float,
           anemometer_height: float, J: int) -> dict:
    """Core FAO-56 Penman-Monteith (+ the four alternative methods), given
    already-aggregated daily inputs - shared by compute_etp (aggregated from
    our own polled observations) and compute_etp_from_summary (aggregated by
    the vendor itself). Returns {et0_mm, et0_alt_mm}."""
    # Adjust wind speed from anemometer height to 2 m (FAO-56 eq. 47)
    if anemometer_height != 2.0 and anemometer_height > 1.0:
        u2 = u_mean * 4.87 / math.log(67.8 * anemometer_height - 5.42)
    else:
        u2 = u_mean

    # Atmospheric pressure (kPa) from elevation (FAO-56 eq. 7)
    P = 101.3 * ((293 - 0.0065 * elevation) / 293) ** 5.26

    # Psychrometric constant γ (kPa/°C) (FAO-56 eq. 8)
    gamma = 0.000665 * P

    # Saturation vapour pressure (kPa) (FAO-56 eq. 11-12)
    es = (_sat_vp(T_max) + _sat_vp(T_min)) / 2

    # Actual vapour pressure (kPa): prefer the station's directly-measured
    # dew point (ASCE (2005) eq. 8) over the RH*es(T_mean) approximation -
    # a measurement beats a derivation from two other measurements.
    if T_dew_mean is not None:
        ea = _sat_vp(T_dew_mean)
    else:
        ea = _sat_vp(T_mean) * RH_mean / 100

    # Slope of saturation vapour pressure curve Δ (kPa/°C) (FAO-56 eq. 13)
    delta = 4098 * _sat_vp(T_mean) / (T_mean + 237.3) ** 2

    # Solar radiation: mean W/m² → MJ/m²/day (FAO-56 §3.3)
    Rs = Rs_wm2 * 86400 / 1e6  # MJ/m²/day

    lat_rad = math.radians(latitude)

    # Extraterrestrial radiation Ra (MJ/m²/day) (FAO-56 eq. 21–28)
    dr      = 1 + 0.033 * math.cos(2 * math.pi * J / 365)
    delta_s = 0.409 * math.sin(2 * math.pi * J / 365 - 1.39)
    omega_s = math.acos(max(-1.0, min(1.0, -math.tan(lat_rad) * math.tan(delta_s))))
    Ra = (24 * 60 / math.pi) * 0.0820 * dr * (
        omega_s * math.sin(lat_rad) * math.sin(delta_s)
        + math.cos(lat_rad) * math.cos(delta_s) * math.sin(omega_s)
    )

    # Clear-sky radiation Rso (FAO-56 eq. 37)
    Rso = max((0.75 + 2e-5 * elevation) * Ra, 0.01)

    # Net shortwave radiation Rns (FAO-56 eq. 38, albedo α=0.23 for grass)
    Rns = (1 - 0.23) * Rs

    # Net longwave radiation Rnl (FAO-56 eq. 39)
    sigma   = 4.903e-9  # MJ/m²/day/K⁴
    rs_rso  = min(Rs / Rso, 1.0)
    Rnl = (
        sigma
        * ((T_max + 273.16) ** 4 + (T_min + 273.16) ** 4) / 2
        * (0.34 - 0.14 * math.sqrt(max(ea, 0.0)))
        * (1.35 * rs_rso - 0.35)
    )

    Rn = Rns - Rnl   # Net radiation (G≈0 for daily)

    # FAO-56 Penman-Monteith (eq. 6)
    ET0 = (
        0.408 * delta * Rn + gamma * (900 / (T_mean + 273)) * u2 * (es - ea)
    ) / (delta + gamma * (1 + 0.34 * u2))
    ET0 = max(ET0, 0.0)

    # MJ/m²/day -> mm/day: divide by the latent heat of vaporisation λ
    # (≈2.45 MJ/kg at ~20°C), i.e. multiply by 1/λ = 0.408.
    MJ_TO_MM = 0.408

    # Hargreaves-Samani (1985): no radiation/humidity/wind sensor needed.
    ET0_hs = max(0.0023 * (T_mean + 17.8) * math.sqrt(max(T_max - T_min, 0.0)) * Ra * MJ_TO_MM, 0.0)

    # Priestley-Taylor (1972): drops the aerodynamic (wind) term; α=1.26
    # is the standard coefficient for well-watered reference vegetation.
    ET0_pt = max(1.26 * (delta / (delta + gamma)) * Rn * MJ_TO_MM, 0.0)

    # Makkink (1957): radiation + temperature only, c=0.61 standard coefficient.
    ET0_mk = max(0.61 * (delta / (delta + gamma)) * Rs * MJ_TO_MM - 0.12, 0.0)

    # Turc (1961): radiation + temperature, with a dryness correction below
    # 50% RH. 23.8846 converts Rs from MJ/m²/day to the cal/cm²/day the
    # original formula uses.
    turc_k = 1.0 if RH_mean >= 50 else 1 + (50 - RH_mean) / 70
    ET0_turc = max(0.013 * (T_mean / (T_mean + 15)) * (23.8846 * Rs + 50) * turc_k, 0.0)

    return {
        "et0_mm": round(ET0, 3),
        "et0_alt_mm": {
            "hargreaves_samani": round(ET0_hs, 3),
            "priestley_taylor":  round(ET0_pt, 3),
            "makkink":           round(ET0_mk, 3),
            "turc":              round(ET0_turc, 3),
        },
    }
