#!/bin/sh

set -e

cd $(dirname "${BASH_SOURCE[0]}")
export CommandsJSON=$(cat commands.json)

# replace out the json
perl -pe '
    while (($i = index($_, "{{.CommandsJSON}}")) != -1) {
      substr($_, $i, length("{{.CommandsJSON}}")) = $ENV{"CommandsJSON"};
    }
' commands_template.go > commands.go

# remove the ignore
sed -i -e 's/\/\/ +build ignore//g' commands.go
rm -rf commands.go-e
gofmt -w commands.go
