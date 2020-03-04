#!/bin/bash

black --line-length 80 pfb_exporter tests
flake8 --ignore=E501,W503,E203 pfb_exporter tests
