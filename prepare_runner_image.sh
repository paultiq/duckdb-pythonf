#!/bin/bash

# Script to prepare a custom GitHub Actions runner image with dependencies pre-installed
set -e

echo "=== Preparing custom GitHub Actions runner image ==="

# Build a custom image with all dependencies pre-installed
docker build -t custom-actions-runner - <<'EOF'
FROM ghcr.io/actions/actions-runner:latest

# Install all the dependencies that take time in the test script
RUN sudo apt-get update -q && \
    sudo apt-get install -y -q zip python3-pip awscli git pipx build-essential python3.11 && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    export PATH="$HOME/.cargo/bin:$PATH" && \
    pip3 install cibuildwheel build

# Clean up to reduce image size
RUN sudo apt-get clean && sudo rm -rf /var/lib/apt/lists/*

EOF

echo "=== Custom runner image 'custom-actions-runner' created successfully ==="
echo "You can now use test_packaging_runner.sh with the faster image"