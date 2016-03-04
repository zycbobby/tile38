#!/bin/bash
set -e

cd $(dirname "${BASH_SOURCE[0]}")
WD=$(pwd)

if [ ! -f "tile38-server" ];then 
	./build.sh
fi

TMP="$(mktemp -d -t data-test.XXXX)"
./tile38-server -p 9876 -d "$TMP" -q &
PID=$!
function end {
  	rm -rf "$TMP"
  	kill $PID &
}
trap end EXIT

go test $(go list ./... | grep -v /vendor/)
