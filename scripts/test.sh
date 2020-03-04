#!/bin/bash

# Abort the script if there is a non-zero error
set -e

. venv/bin/activate
pip install -r dev-requirements.txt
py.test --cov=pfb_exporter tests

# flake8 --ignore=E501,W503,E203 pfb_exporter tests
# black --check --line-length 80 pfb_exporter tests
