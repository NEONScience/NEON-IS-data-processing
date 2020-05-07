#!/usr/bin/env bash
read -r -p "Enter module directory to build: " module
docker build --build-arg APP_DIR="$module" -t "$module":latest -f "$module"/Dockerfile .