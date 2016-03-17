#!/bin/bash
set -e

VERSION="1.0.4"
PROTECTED_MODE="no"

# Hardcode some values to the core package
LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.Version=${VERSION}"
if [ -d ".git" ]; then
	LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.GitSHA=$(git rev-parse --short HEAD)"
fi
LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.BuildTime=$(date +%FT%T%z)"
if [ "$PROTECTED_MODE" == "no" ]; then
	LDFLAGS="$LDFLAGS -X github.com/tidwall/tile38/core.ProtectedMode=no"
fi

if [ "$(which go)" == "" ]; then
	echo "error: Go is not installed. Please download and follow installation instructions at https://golang.org/dl to continue."
	exit 1
fi

vercomp () {
    if [[ $1 == $2 ]]
    then
        echo "0"
        return
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            echo "1"
        	return
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            echo "-1"
            return
        fi
    done
    echo "0"
    return
}

GOVERS="$(go version | cut -d " " -f 3)"
GOVERS="${GOVERS:2}"
EQRES=$(vercomp "$GOVERS" "1.5")  

if [ "$EQRES" == "-1" ]; then
      echo "error: Go '1.5' or greater is required and '$GOVERS' is currently installed. Please upgrade Go at https://golang.org/dl to continue."	
      exit 1
fi

export GO15VENDOREXPERIMENT=1

cd $(dirname "${BASH_SOURCE[0]}")
OD="$(pwd)"

# temp directory for storing isolated environment.
TMP="$(mktemp -d -t tile38.XXXX)"
function rmtemp {
  	rm -rf "$TMP"
}
trap rmtemp EXIT

if [ "$NOCOPY" != "1" ]; then
	# copy all files to an isloated directory.
	WD="$TMP/src/github.com/tidwall/tile38"
	export GOPATH="$TMP"
	for file in `find . -type f`; do
		# TODO: use .gitignore to ignore, or possibly just use git to determine the file list.
		if [[ "$file" != "." && "$file" != ./.git* && "$file" != ./data* && "$file" != ./tile38-* ]]; then
			mkdir -p "$WD/$(dirname "${file}")"
			cp -P "$file" "$WD/$(dirname "${file}")"
		fi
	done
	cd $WD
fi

#core/gen.sh

# build and store objects into original directory.
go build -ldflags "$LDFLAGS" -o "$OD/tile38-server" cmd/tile38-server/*.go
go build -ldflags "$LDFLAGS" -o "$OD/tile38-cli" cmd/tile38-cli/*.go

# test if requested
if [ "$1" == "test" ]; then
	$OD/tile38-server -p 9876 -d "$TMP" -q &
	PID=$!
	function testend {
	  	kill $PID &
	}
	trap testend EXIT
	go test $(go list ./... | grep -v /vendor/)
fi

