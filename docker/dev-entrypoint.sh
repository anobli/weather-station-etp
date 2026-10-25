#!/usr/bin/env bash
# Dev container entrypoint. Nothing project-specific needed yet; kept as a
# hook point (e.g. first-run setup) and for parity with the other Gordios
# dev containers.
set -e

exec "$@"
