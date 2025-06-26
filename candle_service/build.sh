#!/bin/sh
set -e
GOOS=linux GOARCH=amd64 go build -o candle-service