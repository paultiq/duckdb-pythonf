#!/bin/bash

# Script to manually test packaging wheels in GitHub Actions runner environment
# Adapted from your runner script

set -e

echo "=== Starting GitHub Actions Runner to test packaging wheels ==="

# Get the current directory (your project)
PROJECT_DIR=$(pwd)
PROJECT_NAME=$(basename "$PROJECT_DIR")

echo "Project: $PROJECT_NAME"
echo "Project Dir: $PROJECT_DIR"

# Run the GitHub Actions runner container with your project mounted
docker run --rm \
  -v "$PROJECT_DIR:/workspace/$PROJECT_NAME" \
  -w "/workspace/$PROJECT_NAME" \
  custom-actions-runner \
  sh -c "
    echo '=== Using pre-configured runner environment ===' &&
    export PATH=\"\$HOME/.cargo/bin:\$PATH\" &&

    echo '=== Current directory and files ===' &&
    pwd &&
    ls -la &&

    echo '=== Testing pyproject.toml backend settings ===' &&
    python3.11 -c \"
import tomllib
with open('pyproject.toml', 'rb') as f:
    config = tomllib.load(f)
    print('backend-path:', config['build-system'].get('backend-path', 'NOT SET'))
    print('build-backend:', config['build-system']['build-backend'])
    print('before-build:', config.get('tool', {}).get('cibuildwheel', {}).get('before-build', 'NOT SET'))
\" &&

    echo '=== Testing backend import directly ===' &&
    python3 -c 'import duckdb_packaging.build_backend; print(\"✓ Backend import successful\")' || echo '✗ Backend import failed' &&

    echo '=== Installing build dependencies manually ===' &&
    python3 -m pip install setuptools wheel 'scikit-build-core>=0.11.4' 'pybind11[global]>=2.6.0' 'setuptools-scm>=8.0' 'cmake>=3.29.0' 'ninja>=1.10' &&

    echo '=== Testing backend import after deps ===' &&
    python3 -c 'import duckdb_packaging.build_backend; print(\"✓ Backend import successful after deps\")' || echo '✗ Backend import still failed' &&

    echo '=== Testing build command that fails in CI ===' &&
    mkdir -p /tmp/test-wheel &&
    python3 -m build . --wheel --outdir=/tmp/test-wheel --no-isolation -v &&

    echo '=== Success! Listing built wheel ===' &&
    ls -la /tmp/test-wheel/
  "

echo "=== Test completed ==="