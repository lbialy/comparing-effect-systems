#! /bin/bash

set -euo pipefail

VARIANT=${1:-future}
HOST=${2:-https://tapir.softwaremill.com/en/latest/}
SELECTOR=${3:-'div[role=main]'}

# host is just the domain name with no scheme (drop both http and https), slashes replaced by underscores
HOST_DIRNAME=$(echo $HOST | sed 's|^https://||; s|^http://||; s|/$||; s|/|_|g')

rm -fr $HOST_DIRNAME
scala package . -o crawldown -f
./crawldown $VARIANT $HOST $SELECTOR