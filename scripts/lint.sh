#!/usr/bin/env bash

echo "Running pyup_dirs..."
pyup_dirs --py38-plus --recursive google-sheets tests

echo "Running ruff linter (isort, flake, pyupgrade, etc. replacement)..."
ruff check

echo "Running ruff formater (black replacement)..."
ruff format

# echo "Running black..."
# black google-sheets examples tests docs
