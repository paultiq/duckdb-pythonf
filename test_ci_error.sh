#!/bin/bash
# Reproduce the exact CI error locally

set -e

echo "=== Reproducing CI Error ==="
echo "Simulating exactly what CI does:"

# Clean environment - remove any local modifications
rm -rf /tmp/test-ci
mkdir -p /tmp/test-ci
cd /tmp/test-ci

# Copy project like CI does
cp -r /home/ec2-user/git/duckdb-pythonf /tmp/test-ci/project

cd /tmp/test-ci/project

echo "=== Step 1: Install build deps like pyproject.toml before-build ==="
python -m pip install setuptools wheel scikit-build-core>=0.11.4 'pybind11[global]>=2.6.0' setuptools-scm>=8.0 'cmake>=3.29.0' 'ninja>=1.10'

echo "=== Step 2: Try to import backend directly ==="
python -c "import duckdb_packaging.build_backend; print('Backend import successful')" || echo "FAILED: Backend import failed"

echo "=== Step 3: Check backend-path setting ==="
python -c "
import tomllib
with open('pyproject.toml', 'rb') as f:
    config = tomllib.load(f)
    print('backend-path:', config['build-system'].get('backend-path', 'NOT SET'))
    print('build-backend:', config['build-system']['build-backend'])
"

echo "=== Step 4: Test build command that's failing in CI ==="
echo "Running: python -m build /tmp/test-ci/project --wheel --outdir=/tmp/test-wheel --no-isolation -vv"
mkdir -p /tmp/test-wheel
python -m build /tmp/test-ci/project --wheel --outdir=/tmp/test-wheel --no-isolation -vv