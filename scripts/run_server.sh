#!/usr/bin/env bash

uvicorn weatherapi.app:app --workers 2 --host 0.0.0.0 --proxy-headers
