#!/usr/bin/env bash
docker build --build-arg APP_DIR="$1" -t "$1":latest -f "$1"/Dockerfile .