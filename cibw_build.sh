#!/bin/bash

# Simple cibuildwheel script mirroring packaging_wheels.yml
set -e

# Configuration matching GHA workflow
export CIBW_BUILD="cp314t-manylinux_x86_64"
export CIBW_ARCHS="x86_64"
export CIBW_BUILD_VERBOSITY=3

# Use settings from pyproject.toml - no overrides needed
# Environment variables for stable paths and caching
export CIBW_ENVIRONMENT_LINUX="UV_NO_BUILD_ISOLATION=1 TMPDIR=/tmp/duckdb-build TEMP=/tmp/duckdb-build UV_CACHE_DIR=/tmp/duckdb-build/uv-cache UV_PROJECT_ENVIRONMENT=/project/.venv UV_PYTHON=cp314t PYTHONPATH=/project"
export CIBW_ENVIRONMENT_PASS_LINUX="UV_NO_BUILD_ISOLATION TMPDIR TEMP UV_CACHE_DIR UV_PROJECT_ENVIRONMENT UV_PYTHON PYTHONPATH"


# Skip tests for faster builds
export CIBW_TEST_SKIP='*'

echo "Building wheel with cibuildwheel..."
echo "CIBW_BUILD: $CIBW_BUILD"
echo "CIBW_BUILD_FRONTEND: $CIBW_BUILD_FRONTEND"
echo "CIBW_ENVIRONMENT_LINUX: $CIBW_ENVIRONMENT_LINUX"

# Create output directory
mkdir -p wheelhouse

# Run cibuildwheel
cibuildwheel --output-dir wheelhouse

echo "Build complete. Wheels in ./wheelhouse/"