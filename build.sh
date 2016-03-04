#!/bin/bash
set -e

VERSION="0.0.1"
BUILD_TIME=$(date +%FT%T%z)
GIT_SHA=$(git rev-parse --short HEAD)
LDFLAGS="-X github.com/tidwall/tile38/core.Version=${VERSION} -X github.com/tidwall/tile38/core.BuildTime=${BUILD_TIME} -X github.com/tidwall/tile38/core.GitSHA=${GIT_SHA}"

export GO15VENDOREXPERIMENT=1

cd $(dirname "${BASH_SOURCE[0]}")
OD="$(pwd)"

# copy all files to an isloated directory.
TMP="$(mktemp -d -t tile38.XXXX)"
function rmtemp {
  	rm -rf "$TMP"
}
trap rmtemp EXIT
WD="$TMP/src/github.com/tidwall/tile38"
GOPATH="$TMP"

for file in `find . -type f`; do
	if [[ "$file" != "." && "$file" != ./.git* && "$file" != ./data* && "$file" != ./tile38-* ]]; then
		mkdir -p "$WD/$(dirname "${file}")"
		cp -P "$file" "$WD/$(dirname "${file}")"
	fi
done

# build and store objects into original directory.
cd $WD
go build -ldflags "$LDFLAGS" -o "$OD/tile38-server" cmd/tile38-server/*.go
go build -ldflags "$LDFLAGS" -o "$OD/tile38-cli" cmd/tile38-cli/*.go



