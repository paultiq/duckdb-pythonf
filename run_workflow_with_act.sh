#!/bin/bash

# Script to run packaging_wheels.yml using act inside Docker
set -e

echo "=== Running packaging_wheels.yml workflow with act in Docker ==="

# Get the current directory (your project)
PROJECT_DIR=$(pwd)
PROJECT_NAME=$(basename "$PROJECT_DIR")

echo "Project: $PROJECT_NAME"
echo "Project Dir: $PROJECT_DIR"

# Create a minimal event file for workflow_call trigger
cat > /tmp/workflow_call_event.json << 'EOF'
{
  "inputs": {
    "minimal": true,
    "testsuite": "none",
    "duckdb-python-sha": "",
    "duckdb-sha": "",
    "set-version": ""
  }
}
EOF

echo "=== Event file created ==="
cat /tmp/workflow_call_event.json

echo "=== Running act in Docker container ==="

# Run act with the locally installed version
act workflow_call \
  --container-architecture linux/amd64 \
  --eventpath /tmp/workflow_call_event.json \
  --workflows .github/workflows/packaging_wheels_local.yml \
  --job build_wheels \
  --platform ubuntu-24.04=ghcr.io/catthehacker/ubuntu:act-24.04 \
  --verbose

echo "=== Workflow execution completed ==="