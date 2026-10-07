#!/bin/bash
# What CI calls; the command lives in scripts/dev/test.sh.
exec "$(dirname "$0")/../dev/test.sh" fmt "$@"
