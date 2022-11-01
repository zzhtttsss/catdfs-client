#!/bin/bash
cd "$(dirname "$0")" || exit
operationType="$1"
if [ "$operationType" == "add" ] || [ "$operationType" == "get" ] || [ "$operationType" == "move" ] || [ "$operationType" == "rename" ];
then
    go run ./cmd/main.go "$operationType" "--src" "$2" "--des" "$3"
elif [ "$operationType" == "mkdir" ] || [ "$operationType" == "remove" ];
then
    go run ./cmd/main.go "$operationType" "--des" "$2"
elif [ "$operationType" == "list" ] || [ "$operationType" == "stat" ];
then
    if [ ! "$3" ]; then
        go run ./cmd/main.go "$operationType" "--des" "$2"
    else
        go run ./cmd/main.go "$operationType" "--des" "$2" "--mode" "$3"
    fi
else
    echo "Unsupported operation!"
fi
