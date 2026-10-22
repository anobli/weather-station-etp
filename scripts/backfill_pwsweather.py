#!/usr/bin/env python3
"""
Manually backfill PWSWeather/Xweather history into an observations-*.db.

The live service does this automatically when a station starts with an
empty DB (see PWSWeatherListener._run / PWSWEATHER_BACKFILL_DAYS). This
script is for backfilling an explicit range on demand - e.g. to re-fetch a
range recorded before a field was captured (dew_point, latitude,
elevation), or to extend history further back than the automatic backfill
covers. Safe to re-run: it overwrites existing rows for the same timestamps.

Usage:
    PWSWEATHER_API_KEY=... python3 scripts/backfill_pwsweather.py \
        --station PWS_KAZMTPHX --db /data/observations-pwsweather.db \
        --from 2026-10-15 --to 2026-10-22
"""
import argparse
import logging
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database  # noqa: E402
from stations.pwsweather import PWSWeatherListener  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--station", default=os.environ.get("PWSWEATHER_STATION_ID"),
                     help="Station id, e.g. PWS_KAZMTPHX (default: $PWSWEATHER_STATION_ID)")
    ap.add_argument("--db", required=True, help="Path to the observations-*.db to write into")
    ap.add_argument("--from", dest="start", required=True, type=date.fromisoformat, help="First day (YYYY-MM-DD, station-local)")
    ap.add_argument("--to", dest="end", required=True, type=date.fromisoformat, help="Last day, inclusive (YYYY-MM-DD, station-local)")
    ap.add_argument("--anemometer-height", type=float, default=float(os.environ.get("ANEMOMETER_HEIGHT", "2.0")))
    args = ap.parse_args()

    if args.end < args.start:
        ap.error("--to must be on or after --from")

    db = Database(args.db)
    db.init()

    # Credentials/station id are read from PWSWEATHER_API_KEY or
    # PWSWEATHER_CLIENT_ID/_CLIENT_SECRET the same way the live service does.
    st = PWSWeatherListener(db, station_id=args.station)
    st.anemometer_height = args.anemometer_height
    days = [args.start + timedelta(days=i) for i in range((args.end - args.start).days + 1)]
    st.backfill(days)


if __name__ == "__main__":
    main()
