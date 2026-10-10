#!/bin/sh
set -eu
exec "$(dirname -- "$0")/control.sh" test "$@"
