#!/bin/bash
cd "$(dirname "$0")" || exit
operationType="$1"
if [ "$operationType" == "add" ] || [ "$operationType" == "get" ] || [ "$operationType" == "move" ] || [ "$operationType" == "rename" ];
then
    go run ./cmd/main.go "$operationType" "--src" "$2" "--des" "$3"
elif [ "$operationType" == "mkdir" ] || [ "$operationType" == "list" ] || [ "$operationType" == "remove" ] || [ "$operationType" == "stat" ];
then
    go run ./cmd/main.go "$operationType" "--des" "$2"
else
    echo "Unsupported operation!"
fi
