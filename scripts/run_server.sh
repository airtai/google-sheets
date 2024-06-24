#!/usr/bin/env bash

cat <<< "$CLIENT_SECRET" > client_secret.json

prisma migrate deploy
prisma generate

uvicorn google_sheets.app:app --workers 2 --host 0.0.0.0 --proxy-headers
