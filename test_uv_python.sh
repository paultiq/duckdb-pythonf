#!/bin/bash

# Test if UV can install Python 3.14.0rc3
set -e

echo "=== Testing UV Python 3.14 availability ==="

# Install UV if not available
if ! command -v uv &> /dev/null; then
    echo "Installing UV..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

echo "UV version: $(uv --version)"

echo "=== Available Python versions ==="
uv python list

echo "=== Trying to install cpython@3.14.0rc3 ==="
uv python install cpython@3.14.0rc3 || echo "rc3 not available"

echo "=== Trying to install cpython@3.14.0rc2 ==="
uv python install cpython@3.14.0rc2 || echo "rc2 not available"

echo "=== Trying to install cpython@3.14.0rc1 ==="
uv python install cpython@3.14.0rc1 || echo "rc1 not available"

echo "=== Test completed ==="