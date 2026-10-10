#!/bin/sh
exec "$(dirname "$0")/control.sh" stop "$@"
