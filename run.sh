#! /bin/bash

set -euo pipefail

VARIANT=${1:-future}

rm -fr tapir.softwaremill.com_en_latest
scala package . -o crawldown -f
./crawldown $VARIANT https://tapir.softwaremill.com/en/latest/
