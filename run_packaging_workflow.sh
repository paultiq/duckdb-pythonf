#!/bin/bash

# Script to run the packaging_wheels.yml workflow using GitHub Actions runner
set -e

echo "=== Running packaging_wheels.yml workflow in GitHub Actions runner ==="

# Get the current directory (your project)
PROJECT_DIR=$(pwd)
PROJECT_NAME=$(basename "$PROJECT_DIR")

echo "Project: $PROJECT_NAME"
echo "Project Dir: $PROJECT_DIR"

# Run the GitHub Actions runner container and execute the workflow steps
docker run --rm \
  -v "$PROJECT_DIR:/github/workspace" \
  -w "/github/workspace" \
  -e GITHUB_WORKSPACE="/github/workspace" \
  -e RUNNER_WORKSPACE="/github/workspace" \
  -e GITHUB_ACTIONS=true \
  -e CI=true \
  ghcr.io/actions/actions-runner:latest \
  bash -c "
    echo '=== GitHub Actions Runner Environment ===' &&
    echo 'GITHUB_WORKSPACE: $GITHUB_WORKSPACE' &&
    echo 'RUNNER_WORKSPACE: $RUNNER_WORKSPACE' &&
    pwd &&
    ls -la &&

    echo '=== Installing UV (astral-sh/setup-uv@v6) ===' &&
    curl -LsSf https://astral.sh/uv/install.sh | sh &&
    export PATH=\"\$HOME/.local/bin:\$PATH\" &&
    uv --version &&

    echo '=== Setting up environment variables from workflow ===' &&
    export CIBW_ARCHS='x86_64' &&
    export CIBW_BUILD='cp314t-manylinux_x86_64' &&
    export CIBW_BUILD_FRONTEND='build[uv]; args: --no-isolation' &&
    export UV_PYTHON='cp314t' &&
    export UV_PROJECT_ENVIRONMENT='/project/.venv' &&
    export PYTHONPATH='/project' &&

    echo '=== Environment from CIBW_ENVIRONMENT_LINUX ===' &&
    export CMAKE_C_COMPILER_LAUNCHER='' &&
    export CMAKE_CXX_COMPILER_LAUNCHER='' &&
    export CFLAGS='-Wno-attributes' &&
    export CXXFLAGS='-Wno-attributes' &&
    export SCCACHE_BASEDIR='/project' &&
    export TMPDIR='/tmp/duckdb-build' &&
    export TEMP='/tmp/duckdb-build' &&
    export UV_NO_BUILD_ISOLATION=1 &&
    export PYTHONPATH='/project' &&
    export UV_CACHE_DIR='/tmp/duckdb-build/uv-cache' &&
    export UV_PROJECT_ENVIRONMENT='/project/.venv' &&
    export UV_PYTHON='cp314t' &&

    echo '=== CIBW_BEFORE_BUILD_LINUX step ===' &&
    mkdir -p /tmp/duckdb-build /tmp/pip-cache &&

    echo '=== Installing cibuildwheel ===' &&
    uv tool install cibuildwheel &&
    export PATH=\"\$HOME/.local/bin:\$PATH\" &&

    echo '=== Running cibuildwheel (pypa/cibuildwheel@v3.1) ===' &&
    export CIBW_TEST_SKIP='*' &&
    mkdir -p /github/workspace/wheelhouse &&
    cibuildwheel --output-dir /github/workspace/wheelhouse
  "

echo "=== Workflow completed. Check wheelhouse/ for results ==="