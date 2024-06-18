#!/usr/bin/env bash

echo "Running pyup_dirs..."
pyup_dirs --py38-plus --recursive weatherapi tests

echo "Running ruff linter (isort, flake, pyupgrade, etc. replacement)..."
ruff check

echo "Running ruff formater (black replacement)..."
ruff format

# echo "Running black..."
# black weatherapi examples tests docs
