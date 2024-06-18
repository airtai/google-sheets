#!/usr/bin/env bash

uvicorn google_sheets.app:app --workers 2 --host 0.0.0.0 --proxy-headers
