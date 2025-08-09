#!/usr/bin/env bash
set -e
[ -f .env ] && export $(grep -v "^#" .env | xargs) || true
( cd market_alerts && go run main.go )
